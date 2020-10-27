# Development


This document contains information for developers who want to contribute to the the project.

## General information

Reload is packaged as a modular Maven project. If you want to modify and compile the source code and run the tests defined in the Junit modules some software must be installed on your development system:

1. *Maven*
2. *Docker*
3. *Docker-compose*

Docker and dockers are used for testing and allow you to create instances of DBMs and other necessary software quickly and easily. 
But in alternative, you can use software instances already installed on your system, if you have them.


## Get the code

Reload is a GitHub public project. Feel free to follow the project and download, analyze and modify the code.

Master branch on GitHub is protected and you can merge on this branch only after an approved pool request. Please work on personal branch or fork the entre project and then release upgrades as pool requests. 

Reload project is released with Apache 2.0 license, so if you create new files remember to insert the license header.  If you add an external library dependency to project, please control license rights before release software modifications.


## Compilation

To build the entire project go in the root and launch this command:

     mvn -Dmaven.test.skip=true package
     
This command compile all the code without tests execution.

## Compilation and test execution

To compile the entire project and run the tests, you must first activate the software needed to run the tests.

1. Go to *docker/mysql* directory and start a docker mysql instance with the command:

       docker-compose up -d
    
2. Go to *docker/rabbitmq* directory and start a docker rabbitmq instance with the command:

       docker-compose up -d

3. Compile entire project and execute tests with the command:

       mvn package    

