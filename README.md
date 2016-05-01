# A simple replicated network file system

This is an implementation of replicated network file system. It uses Raft consensus algorithm for replication. Each file has a version number, and the server keeps the latest version. There are four commands, to read, write, compare-and-swap and delete the file. Files have an optional expiry time attached to them. 

## Installing and Testing

```
go get github.com/agrawalpooja/cs733-Assignment4/
go test github.com/agrawalpooja/cs733-Assignment4/
```
For dependencies do

```
go get github.com/cs733-iitb/log
go get github.com/cs733-iitb/cluster
```


## Command Specification

The format for each of the four commands is shown below,  

| Command  | Success Response | Error Response
|----------|-----|----------|
|read _filename_ \r\n| CONTENTS _version_ _numbytes_ _exptime remaining_\r\n</br>_content bytes_\r\n </br>| ERR_FILE_NOT_FOUND
|write _filename_ _numbytes_ [_exptime_]\r\n</br>_content bytes_\r\n| OK _version_\r\n| |
|cas _filename_ _version_ _numbytes_ [_exptime_]\r\n</br>_content bytes_\r\n| OK _version_\r\n | ERR\_VERSION _newversion_
|delete _filename_ \r\n| OK\r\n | ERR_FILE_NOT_FOUND

In addition the to the semantic error responses in the table above, all commands can get two additional errors. `ERR_CMD_ERR` is returned on a malformed command, `ERR_INTERNAL` on, well, internal errors. On getting `ERR_REDIRECT` close your current connection and connect to given node-id/socket address.

For `write` and `cas` and in the response to the `read` command, the content bytes is on a separate line. The length is given by _numbytes_ in the first line.

Files can have an optional expiry time, _exptime_, expressed in seconds. A subsequent `cas` or `write` cancels an earlier expiry time, and imposes the new time. By default, _exptime_ is 0, which represents no expiry. 



## Limitations

- Ports are fixed at 8085, 8086, 8087, 8088, 8089
- Files are in-memory. There's no persistence.
- If the command or contents line is in error such that the server
  cannot reliably figure out the end of the command, the connection is
  shut down. Examples of such errors are incorrect command name,
  numbytes is not not  numeric, the contents line doesn't end with a
  '\r\n'.
- The first line of the command is constrained to be less than 500 characters long. If not, an error is returned and the connection is shut down.