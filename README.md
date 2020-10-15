# Execute Stored Procedure
This Smart Service allows a stored procedure to be executed while mapping data in and out. Result sets are returned as CDTs.

See the instructions document for an example and how to configure.

### Development Build
To build the plug-in for testing locally run the following command:
1. Run `mvn clean package`
1. The plug-in jar can be found in `/target`
1. See the `/src/test/resources` folder for example SQL and Appian expressions

### Release Build
To create a new public release, run the following Maven commands:
1. `mvn release:clean`
1. `mvn release:prepare -DpushChanges=false`
1. `git push origin master --tags`
1. The release plug-in jar can be found in `/target`
1. Test and upload to Shared Components