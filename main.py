import subprocess

# Execute GA 4 analytics sessions script
subprocess.run(["python", "scripts/ga_sitesession.py"])

# Execute DB leads aggregated by site script
subprocess.run(["python", "scripts/leads_a1.py"])

# Execute report A1 generation script
subprocess.run(["python", "scripts/report_a1.py"])
