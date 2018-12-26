base=~/projects/development/SOC_scripts/careem
curr=~/projects/development/SOC_scripts/careem/bruteforce

source $base/.venv/bin/activate

prev=$pwd;
cd $curr;
python script.py >> .run.logs 2>&1
cd $prev;
