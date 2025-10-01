import json
from servir.core.data_provider import IMERGDataModuleLP
from servir.core.model_picker import ModelPicker
import h5py
import numpy as np
from servir.core.data_provider import ImergGhanaMonthlyDataset
import os 
from torch.utils.data.dataloader import DataLoader
import torch

def dataloader_test():
    
    data_provider =  IMERGDataModuleLP(
        batch_size = 32,
        image_shape = (64,64),
        normalize_data=False,)

    # test_data_loader = data_provider.test_dataloader()
    test_data_loader = data_provider.val_dataloader()
    

    for data_sample in test_data_loader:
        x , x_ir, y = data_sample
        # print(x.shape, x_ir.shape, y.shape)
        
        

def main():
    
    h5_dataset_location = "temp/ghana_imerg_2011_2020_Oct.h5"
    ir_h5_dataset_location = "temp/ghana_IR_2011_2020_oct.h5"
    model_type = 'lagrangian'
    model_config_location = 'configs/gh_imerg/lagrangian_persistence.py'
    model_picker = ModelPicker(model_type, model_config_location)
    model_picker.load_model(get_ensemble=False)
    forecast_steps = 12
    history_steps = 8
    image_shape = (64,64)
    

    with h5py.File(h5_dataset_location, 'r') as hf:
        precipitation_time_series = hf['precipitations'][:].astype(np.float32)
    with h5py.File(ir_h5_dataset_location, 'r') as hf:
        IR_time_series = hf['IRs'][:].astype(np.float32)
    
    
    train_dataset = ImergGhanaMonthlyDataset(precipitation_time_series, 
                                    IR_time_series, 
                                    0, 
                                    8, 
                                    forecast_steps, 
                                    history_steps,
                                    image_shape=image_shape)
    val_dataset = ImergGhanaMonthlyDataset(precipitation_time_series, 
                                        IR_time_series, 
                                        8, 
                                        9, 
                                        forecast_steps, 
                                        history_steps,
                                        image_shape=image_shape)
                
    test_dataset = ImergGhanaMonthlyDataset(precipitation_time_series, 
                                IR_time_series, 
                                9, 
                                10, 
                                forecast_steps, 
                                history_steps,
                                image_shape=image_shape)
    train_dataloader = DataLoader(train_dataset, batch_size=1, num_workers=2)
    val_dataloader = DataLoader(val_dataset, batch_size=1, num_workers=2)
    test_dataloader = DataLoader(test_dataset, batch_size=1, num_workers=2)
    
    

    updated_x = None
    updated_x_ir = None
    updated_y = None
    updated_lp_output = None
    
    dataset = 'train'
    
    hf1 =  h5py.File('temp/{}_ir.h5'.format(dataset), 'w')
    hf2 =  h5py.File('temp/{}_input.h5'.format(dataset), 'w')
    hf3 =  h5py.File('temp/{}_output.h5'.format(dataset), 'w')
    hf4 =  h5py.File('temp/{}_lp_output.h5'.format(dataset), 'w')
    
    for index, sample in enumerate(train_dataloader):
        print(index)
        x, x_ir, y = sample
        predicted_precip = model_picker.predict(x[0, :, 0, :, :].numpy())
        predicted_precip = predicted_precip[None, :,None, :, :]
        precip_difference = predicted_precip - y.numpy()
        
        if updated_x is None:
            updated_x = x
            updated_x_ir = x_ir
            updated_y = precip_difference
            updated_lp_output = predicted_precip
            # write to h5 file
            ir_dataset = hf1.create_dataset('{}_ir'.format(dataset), data = updated_x_ir, maxshape=(None, 16,1,64,64))
            input_dataset = hf2.create_dataset('{}_input'.format(dataset), data = updated_x, maxshape=(None, 8,1,64,64))
            y_dataset = hf3.create_dataset('{}_y'.format(dataset), data = updated_y, maxshape=(None, 12,1,64,64))
            lp_output_dataset = hf4.create_dataset('{}_lp_output'.format(dataset), data = updated_lp_output,  maxshape=(None, 12,1,64,64))
            
            # with h5py.File('temp/{}_ir.h5'.format(dataset), 'w') as hf:
            #     ir_dataset = hf.create_dataset('{}_ir'.format(dataset), data = updated_x_ir)
            # with h5py.File('temp/{}_input.h5'.format(dataset), 'w') as hf:
            #     input_dataset = hf.create_dataset('{}_input'.format(dataset), data = updated_x)
            # with h5py.File('temp/{}_output.h5'.format(dataset), 'w') as hf:
            #     y_dataset = hf.create_dataset('{}_y'.format(dataset), data = updated_y)
            # with h5py.File('temp/{}_lp_output.h5'.format(dataset), 'w') as hf:
            #     lp_output_dataset = hf.create_dataset('{}_lp_output'.format(dataset), data = updated_lp_output)
        else:
            ir_dataset.resize(ir_dataset.shape[0]+1, axis=0)
            ir_dataset[-1:] = x_ir
            
            
            input_dataset.resize(input_dataset.shape[0]+1, axis=0)
            input_dataset[-1:] = x
            
            y_dataset.resize(y_dataset.shape[0]+1, axis=0)
            y_dataset[-1:] = precip_difference
            
            lp_output_dataset.resize(lp_output_dataset.shape[0]+1, axis=0)
            lp_output_dataset[-1:] = predicted_precip
            
            # updated_x = np.concatenate((updated_x, x))
            # updated_x_ir = np.concatenate((updated_x_ir, x_ir))
            # updated_y = np.concatenate((updated_y, precip_difference))
            # updated_lp_output = np.concatenate((updated_lp_output, predicted_precip))
        
    hf1.close()
    hf2.close()
    hf3.close()
    hf4.close()

if __name__ == "__main__":
    main()
    # dataloader_test()