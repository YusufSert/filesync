### PGM FILE SYNC

# 🚀 Service Description

This service synchronizes files between a standard **FTP server (not SFTP)** and the local file system, which may include Windows shared directories. It is implemented as a **concurrent application** with two dedicated threads:

- 🔽 **Download thread**: synchronizes files from the FTP server to the local file system.
- 🔼 **Upload thread**: synchronizes files from the local file system back to the FTP server.

These threads run concurrently to enable efficient, bidirectional file synchronization between the FTP server and the local environment.

✅ **To ensure reliability**:

- 🔎 The service **monitors each thread’s health and progress**. If a thread **crashes** or **becomes unresponsive** (e.g., takes too long to process), it will automatically terminate the thread and start a new sync thread to continue synchronization without manual intervention.
- 🔁 **Retry and exponential backoff patterns** are used when errors occur (e.g., network issues, temporary FTP failures) to make the service more resilient and minimize the risk of permanent failures due to transient problems.


# ⚙️ Configuration Description 📋

| 🔧 **Parameter**      | 📝 **Description**                                                                                   |
|----------------------|-----------------------------------------------------------------------------------------------------|
| **addr**             | The FTP server address or hostname where the service connects to synchronize files.                  |
| **user**             | Username credential for authenticating with the FTP server.                                          |
| **password**         | Password credential corresponding to the FTP user.                                                   |
| **dbConnStr**        | Database connection string used by the service to connect to the database.                            |
| **networkBasePath**  | The root local network path or directory from which files are synchronized.                          |
| **networkToUploadPath** | Directory path for files to be uploaded from local to FTP.                                        |
| **networkOutgoingPath** | Directory path on the local system where files are moved after successfully being uploaded.        |
| **networkIncomingPath** | Directory path for files incoming to the local system.                                             |
| **ftpWritePath**     | The directory path on the FTP server where files will be uploaded.                                   |
| **ftpReadPath**      | The directory path on the FTP server from which files will be downloaded.                            |
| **poolInterval**     | Time interval (e.g., 5s for 5 seconds) between synchronization cycles or polling attempts.           |
| **heartbeatInterval** | Time interval (e.g., 1s for 1 second) used to send heartbeat signals for monitoring thread health or service status. |
| **logFilePath**      | File path where logs will be written. The file will be created automatically if it does not exist.    |
| **logLevel**         | Logging verbosity level (e.g., -4), controlling the amount of detail in logs.                        |
| **lokiPushURL**      | URL endpoint of a Loki server to which logs are pushed for centralized logging.                      |

# Build Instructions

## Clone the repository form TFS
```bash
git clone http://kctfs:8080/tfs/DefaultCollection/Benefit4C/_git/PGM
```

## Install Go 1.24

## Run commands below for debendencies
```bash
 go mod download
```

## 1. Build for Linux

Run the following command to build the Linux version of the service.  
The output binary will be placed in the `bin` directory with the name `pgmsync`:

```bash
go build -o bin/pgmsync ./cmd/linuxservice/main.go
```

## 2. Build for Windows
Run the following command to cross-compile the Windows version of the service.
The output executable will be named pgmsync.exe and placed in the bin directory

```bash
GOOS=windows GOARCH=amd64 go build -o bin/pgmsync.exe ./cmd/windowsservice/main.go
```
## 3. Build for Docker
Build your Go application inside a Docker container to ensure a consistent and reproducible build environment.

```bash
cd <where docker file located>
docker build -t pgmsync:0.1 .
dockerun run --name pgmsync -v <config.yml inside host>:/<config.yml inside container>:ro  --resatart unless-stopped pgmsync:0.1 \
-config.path=<bind config file>
```


# Logging

## Grafana and Loki

Our Grafana dashboard is currently accessible at:

http://172.26.98.110:3000/

At present, user authentication is handled directly by Grafana’s built-in authentication system.

In the future, we plan to enhance security by integrating LDAP authentication, enabling centralized user management and streamlined access control.

