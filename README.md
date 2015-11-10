# ASD
Trabalho 2 de ASD

# To launch a actor system
```bash
sbt "run-main deploy.Base $actorSystemName"
```
# How to Deploy servers and clients

Edit the file base.conf

Run:
```bash
 sbt "run-main deploy.Deployer $remote0 $remote1 $remote2"
```
Attention: Each arg must match a name in the file base.conf
