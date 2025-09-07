# docker-node-js-webpack
Webpack Node.js Application Running Inside Docker

# Docker
```bash
docker run -d -p 8080:8080 dev3l/docker-node-js-webpack
```

Retrieve the IP address of the machine in Windows / Mac usings the following command:
```bash
docker-machine ip default
curl http://192.168.99.100:3000
```


# Project Setup
* npm install


# Local/Dev Use 
* npm run start
  * Runs a local server with an instance of the application available
  * http://localhost:3000
* npm run production
  * Creates ./dist directory and necessary
