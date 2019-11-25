# Running the Solution

### Note

For the sake of keeping this exercise as concise as possible I have put all code into one file. If this
were a normal project I would break this out into modules.

### Step 1

From the root of the repository build the image by doing:
```
docker build -t hinge_hw -f docker/postgres/Dockerfile
```

### Step 2

Create a virtual environment for Python. I wrote this using Python 3.5 so any version that is
greater or equal to that should suffice. Something to the following extent on your machine:

```
python3.5 -m venv hinge_hw/

source hinge_hw/bin/activate
```

You can do `which python3` to try and find which version you have. Typically it will either be available as
`python{major}.{minor}` or `python{major}{minor}` so for example, Python 3.5 can be installed on your machine
as either `python3.5` or `python35`

### Step 3

Install the packages needed in your virtual environment, `pip install -r requirements.txt`

### Step 4

Spin up the Docker image, `docker run -d hinge_hw:latest -p 5432:5432`

### Step 5

Execute the Python code, `python solution.py`