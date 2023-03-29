# Deployment instructions
Running the deployment script in powershell will create a unqiue resource group with all required resources in it. The user deploying the script is required to have permission to do programatic deployments in the selected subscription.

Make sure to sign in to with az cli using the following command and switch to your subscription where you would like to deploy the lab.

<br>

```
az login 
az account set -s <subscription id>
```

<br>

Consider updating the location of the deployment to a region close to your location.

![image](https://user-images.githubusercontent.com/70010056/228456943-68b0d9bf-1253-4f53-bda5-fdc6f474ec9a.png)

Deploy the infrastructure running the deployment script 

```
deployment.ps1
```
