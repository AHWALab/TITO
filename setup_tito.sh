## Set up config file
# Create conda environment from the tito_env.yml file.
echo "Creating conda environment from tito_env.yml..."
conda env create -f tito_env.yml 
# Activate the conda environment
conda activate tito_env

echo "Installing ML libraries..."
cd Nowcast/nowcasting/
pip install -e . 
conda install -y requests
cd ../../

echo "Environment installed successfully..."