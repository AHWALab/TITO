## Set up config file

# Clone the GitHub repository 
echo "Cloning the GitHub repository..."
git clone https://github.com/FLOWARN/nowcasting.git
mkdir ML
mv nowcasting/ ./ML
# Change directory
cd ML/nowcasting/

# Create conda environment from the tito_env.yml file.
echo "Creating conda environment from tito_env.yml..."
conda env create -f tito_env.yml 
# Activate the conda environment
conda activate tito_env
# Install pip packages from the orchestrator packages (if using orchestrator)
pip install -r pip_requirements.txt
# Install the servir package locally
pip install -e .
# move the servir_nowcasting_examples directory to the parent directory
mv servir_nowcasting_examples/ ../

# Create necessary directories
echo "Creating necessary directories..."
mkdir -p ../servir_nowcasting_examples/temp
mkdir -p data
mkdir -p ../servir_nowcasting_examples/results
mkdir -p ../../precip
mkdir -p ../../precipEF5
mkdir -p ../../qpf_store