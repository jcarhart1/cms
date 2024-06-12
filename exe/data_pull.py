import subprocess
import os

# Define the paths to the scripts
scripts = [
    os.path.join('..', 'lib', 'beneficiary_data_unzip_local_store.py'),
    os.path.join('..', 'lib', 'inpatient_data_unzip_local_store.py'),
    os.path.join('..', 'lib', 'outpatient_data_unzip_local_store.py')
]


# Function to run a script
def run_script(script):
    result = subprocess.run(['python', script], capture_output=True, text=True)
    print(f"Running {script}")
    print(result.stdout)
    if result.stderr:
        print(f"Error in {script}: {result.stderr}")


# Run each script
for script in scripts:
    run_script(script)

