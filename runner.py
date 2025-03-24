import yaml
import subprocess

with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

scripts = config.get("scripts", [])

for script in scripts:
    print(f"Running script: {script}")
    result = subprocess.run(["python", script], capture_output=True, text=True)

    print(result.stdout)
    # Handle errors
    if result.returncode != 0:
        print(f"Error executing {script}: {result.stderr}")
        break  # Stop execution if a script fails