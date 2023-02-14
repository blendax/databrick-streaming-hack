# Deployment instructions
Running the deployment script in powershell will create a unqiue resource group with all required resources in it. The user deploying the script is required to have permission to do programatic deployments.  

Make sure to sign in to with az cli using the following command and switch to your subscribtion where you would like to deploy the lab.

<br>

```
az login 
az account set -s <subscription id>
```

<br>

Consider updating the location of the deployment to a region close to your location

image of 

