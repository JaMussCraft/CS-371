# CS-371
This repo is for CS 371 programming assignments done by James, Ayah, and Hannah. 

## PA1

### Compilation
```bash
gcc -o pa1_skeleton pa1_skeleton.c -pthread 
```

### Starting Server
```bash
./pa1_skeleton server 127.0.0.1 12345
```

### Running Clients
```bash
./pa1_skeleton client 127.0.0.1 12345 4 1000000
```

## PA2 - task 1

### Compilation
```bash
gcc -o pa2_task1 pa2_task1.c -pthread 
```

### Starting Server
```bash
./pa2_task1 server 127.0.0.1 12345
```

### Running Clients
```bash 
# this should led to packet loss half the time
./pa2_task1 client 127.0.0.1 12345 15000 5 
```
