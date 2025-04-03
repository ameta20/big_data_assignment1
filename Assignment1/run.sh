#!/bin/bash

#SBATCH --job-name=bigdata1   # Job name appearing in the queue
#SBATCH --time=0-01:00:00            # <<< SET TIME LIMIT (D-HH:MM:SS) - e.g., 8 hours. ADJUST AS NEEDED!
#SBATCH --nodes=1                    # Number of nodes for this script (usually 1)
#SBATCH --ntasks=1                   # Number of tasks (usually 1 for a script)
#SBATCH --cpus-per-task=1            # CPUs for this script (usually 1)
#SBATCH --partition=compute          # <<< REPLACE 'compute' with a VALID partition on YOUR HPC cluster
#SBATCH --qos=normal                 # Quality of Service (adjust or remove based on YOUR HPC cluster)
#SBATCH --output=slurm_wikiscale_%j.log # Combined output and error log file (%j = Job ID)
#SBATCH --error=slurm_wikiscale_%j.log  # Send errors to the same file as output

################################################################################
#                            SETUP ENVIRONMENT                                 #
################################################################################


# --- Load necessary modules ---
module load  tools/Hadoop/2.10.0-GCCcore-10.2.0-native-Java-1.8


################################################################################
#                            EXECUTE THE SCRIPT                              #
################################################################################

echo "Starting the scalability test script (run_scalability_test.sh)..."
echo "Detailed timings will also be logged to scalability_runtimes.log"
echo

# Make sure the script is executable (redundant if already done, but safe)
chmod +x run_scalability_test.sh

# Execute the script that runs the iterative Hadoop jobs
./run_scalability_test.sh

# Check the exit status of the script
SCRIPT_EXIT_CODE=$?
if [ $SCRIPT_EXIT_CODE -eq 0 ]; then
    echo "Scalability test script finished successfully."
else
    echo "ERROR: Scalability test script finished with exit code $SCRIPT_EXIT_CODE."
fi

echo "============================================================"
echo "SLURM Job finished at: $(date)"
echo "============================================================"

exit $SCRIPT_EXIT_CODE
