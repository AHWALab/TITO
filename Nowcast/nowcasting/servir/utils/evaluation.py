import json
from servir.core.model_picker import ModelPicker
from pysteps.verification.probscores import CRPS
import numpy as np
import json
from servir.core.data_provider import IMERGDataModule
from pysteps.utils.spectral import rapsd
from pysteps.verification.detcatscores import det_cat_fct
import torch

def evaluation(metadata_location, data_loader, thr, model_type, model_config_location, model_save_location, use_gpu, use_ensemble=False):
    
    with open(metadata_location) as jsonfile:
        geodata_dict = json.loads(jsonfile.read())

    crps_dict = {model_type:{},
                 'gt':{}
                }

    psd_dict = {model_type:{},
                 'gt':{}
                }

    csi_dict = {model_type:{},
                 'gt':{}
                }

    for j in range(12):
        crps_dict[model_type][str(j)] = []
        crps_dict['gt'][str(j)] = []
        
        psd_dict[model_type][str(j)] = []
        psd_dict['gt'][str(j)] = []
        
        csi_dict[model_type][str(j)] = []
        csi_dict['gt'][str(j)] = []
        
    if use_ensemble:
        model_picker = ModelPicker(model_type, model_config_location, model_save_location, use_gpu)
        model_picker.load_model()

        for index, data_sample_batch in enumerate(data_loader):
            x, y = data_sample_batch
            print("starting predictions for batch {}".format(index))
            if model_type in ['steps', 'lagrangian', 'naive', 'linda']:
                x = x.numpy()[:,:,0,:,:]
                y = y.numpy()[:,:,0,:,:]

                for data_sample_index in range(len(x)):
                    try:
                        predicted_output = model_picker.predict(np.nan_to_num(x[data_sample_index]))
                        for j in range(12):
                            crps_dict[model_type][str(j)].append(CRPS(predicted_output[:,0:j,: ], y[data_sample_index][0:j]))
                    except:
                        pass
            elif model_type in ['dgmr']:
                predicted_output = torch.tensor(model_picker.predict(x))
                rearanged_output = predicted_output.numpy().transpose(1, 0, 2, 3, 4, 5)[:,:,:,0,:,:]
                for data_sample_index in range(len(rearanged_output)):
                    output = rearanged_output[data_sample_index]
                    for j in range(12):
                        crps_dict[model_type][str(j)].append(CRPS(output[:,0:j,:,:], y.numpy()[:,:,0,:,:][data_sample_index][0:j,:,:]))
                    
            elif model_type in ['dgmr_ir']:
                predicted_output = torch.tensor(model_picker.predict(x, x_ir))
                print(predicted_output.shape)

                rearanged_output = predicted_output.numpy().transpose(1, 0, 2, 3, 4, 5)[:,:,:,0,:,:]
                for data_sample_index in range(len(rearanged_output)):
                    output = rearanged_output[data_sample_index]
                    for j in range(12):
                        crps_dict[model_type][str(j)].append(CRPS(output[:,0:j,:,:], y.numpy()[:,:,0,:,:][data_sample_index][0:j,:,:]))
            

        np.save(model_type + '_crps.npy',crps_dict[model_type])

    model_picker = ModelPicker(model_type, model_config_location,model_save_location, use_gpu)
    model_picker.load_model(get_ensemble=False)
    errored_out = 0

    for index, data_sample_batch in enumerate(data_loader):
        x, y = data_sample_batch
        print("starting predictions for batch {}".format(index))
        if model_type in ['steps', 'lagrangian', 'naive', 'linda']:
            x = x.numpy()[:,:,0,:,:]
            y = y.numpy()[:,:,0,:,:]

            for data_sample_index in range(len(x)):
                try:
                    predicted_output = np.nan_to_num(model_picker.predict(x[data_sample_index]))
                    for j in range(12):
                        csi_dict[model_type][str(j)].append(det_cat_fct(predicted_output[0,0:j,: ], y[data_sample_index][0:j], thr=thr)['CSI'])
                        psd_dict[model_type][str(j)].append(rapsd(predicted_output[0,j,:], return_freq=True, fft_method = np.fft))
                        psd_dict['gt'][str(j)].append(rapsd(y[data_sample_index][0:j], return_freq=True, fft_method = np.fft))
                        
                except:
                    errored_out += 1
        elif model_type in ['dgmr', 'convlstm']:
            print("here")
            predicted_output = torch.tensor(model_picker.predict(x))
            print(predicted_output.shape)

            rearanged_output = predicted_output.numpy().transpose(1, 0, 2, 3, 4, 5)[:,:,:,0,:,:]
            for data_sample_index in range(len(rearanged_output)):
                output = rearanged_output[data_sample_index]
                for j in range(12):
                    csi_dict[model_type][str(j)].append(det_cat_fct(output[0][0:j,:,:], y.numpy()[:,:,0,:,:][data_sample_index][0:j,:,:], thr=thr))
                    psd_dict[model_type][str(j)].append(rapsd(output[0][j],return_freq=True, fft_method = np.fft))
                    psd_dict['gt'][str(j)].append(rapsd(y.numpy()[:,:,0,:,:][data_sample_index][j,:,:],return_freq=True, fft_method = np.fft))
                    
        elif model_type in ['dgmr_ir']:
            predicted_output = torch.tensor(model_picker.predict(x, x_ir))
            print(predicted_output.shape)

            rearanged_output = predicted_output.numpy().transpose(1, 0, 2, 3, 4, 5)[:,:,:,0,:,:]
            for data_sample_index in range(len(rearanged_output)):
                output = rearanged_output[data_sample_index]
                for j in range(12):
                    csi_dict[model_type][str(j)].append(det_cat_fct(output[0][0:j,:,:], y.numpy()[:,:,0,:,:][data_sample_index][0:j,:,:], thr=thr))
                    psd_dict[model_type][str(j)].append(rapsd(output[0][j], return_freq=True, fft_method = np.fft))
                    psd_dict['gt'][str(j)].append(rapsd(y.numpy()[:,:,0,:,:][data_sample_index][j,:,:], return_freq=True, fft_method = np.fft))


    np.save(model_type + '_rapsd.npy',psd_dict[model_type])
    np.save('gt_rapsd.npy',psd_dict['gt'])
    np.save(model_type + '_' +str(thr) + '_csi.npy',csi_dict[model_type])
    print("total errored out = ", errored_out)
    
    return crps_dict, psd_dict, csi_dict