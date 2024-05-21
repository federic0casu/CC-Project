# CC-Project
Analyze letter frequency through Hadoop


# How to Hadoop

To copy your MapReduce program, use `scp`.

1st step: package your app with `maven`. Execute the following cmd from the directory on which resides `pom.xml`
```sh
$ mvn clean package
```

2nd step: copy the newly packaged app onto the virtual machine:
```sh
$ scp targert/Name-of-MapReduce-app-1.0-SNAPSHOT.jar hadoop@10.1.1.103:~/CC-Project/jars/
```

3rd step: use the script `./start-hadoop-run.sh`:
```sh
$ ./start-hadoop-run.sh Name-of-MapReduce-app-1.0-SNAPSHOT.jar
```