# COSC 6339 HW2 - Team 12

## Team Members

Kapoor, Kartik  2462 \
Nham, Bryan 2494 \
Panyala, Sukrutha 8740 \
Vohra, Vedant 2889

## Task

**Max Clique**

## How to run the program

The script uses a config file to connect to the database.

Create a new text file called `connection.cfg` in the same directory as the script with the contents in the following format:
```yaml
[DATABASE]
host=127.0.0.1
port=5433
user=<current_vertica_username>
password=<current_vertica_user_password>
database=<database_name>

# Add the following lines ONLY if you are using SSH tunneling 
# (i.e. running from outside the server)
[SSH]
host=cs6339.cs.uh.edu
localhost=127.0.0.1
port=5433
ssh_username=<your_ssh_username>
ssh_password=<your_ssh_password>
```

Before running the script, run:
```bash
pip3 install -r requirements.txt
```

Syntax for program call:
```bash
python3 graphsql.py "task=clique;table=cliqueTest;source=i;destination=j;k=3"
```
*Note: If `task=clique+validate` is passed, the validation script will be run after the query generates the result.*

* The generated SQL code will be saved in `graphsql.sql` in the same directory as the script.
* Any warnings or mild errors will be logged to `graphsql.log` in the same directory as the script.
