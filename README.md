# bexh-trade-executor-aws-ecs

## Project setup:

Virtual environment setup:

```
pip3 install pipenv
pipenv shell --python 3.8
```

Install pipfile libraries:

```
make install-dev
```

### Set up interpreter:
```
1. Go to PyCharm > Settings > Project:bexh-api-aws-lambda > Project Interpreter

2. Select ... to the right of the option and select add...

3. Select existing environment

4. Select the python file as per your virtual environment which can be found in terminal using:
    
    which python

5. Apply, OK
```

## Configuring .env File
```
1. Go to Preferences > Plugins andinstall the EnvFile plugin

2. Restart PyCharm
```

## Running the Project
```
1. Open service.py in one of the apps in main/src/app and hit the play button on the left hand side which will
use the test json from main/test/resources as the event to your function

2. Double check that working directory is set to your path to bexh-email-aws-lambda

2. At the top right of the screen, drop down the service python configuration
and select "edit configurations..."

3. Select the EnvFile tab at the top, enable EnvFile, add new env file, and select .env

4. Apply, OK
```

## Help

Make Targets:
```
Commands:
    test           Runs all test cases
    install        Installs pipfile libraries
    install-dev    Installs pipfile libraries for development purposes
    docker-up      Runs redis and redis commander locally
    docker-down    Tear down redis and redis commander
    local-setup    Spin up local resources
```
