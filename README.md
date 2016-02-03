# nifi-streaming-examples
Collection of examples integrating NiFi with stream process frameworks.

## Initial Setup

* Download the latest [Apache NiFi release](https://nifi.apache.org/download.html)

* Extract the tar and create two instances of NiFi:
  <pre><code>  
    tar xzvf nifi-<version>-bin.tar.gz
    mv nifi-<version> nifi-edge
    tar xzvf nifi-<version>-bin.tar.gz
    mv nifi-<version> nifi-core
  </code></pre>
* Configure the edge instance by editing nifi-edge/conf/nifi.properties and setting the following properties:
  <pre><code>  
    nifi.remote.input.socket.port=7088
    nifi.remote.input.secure=false
    nifi.web.http.port=7080
  </code></pre>
* Configure the core instance by editing nifi-core/conf/nifi.properties and setting the following properties:
  <pre><code> 
    nifi.remote.input.socket.port=8088
    nifi.remote.input.secure=false
    nifi.web.http.port=8080
  </code></pre> 
* Start both instances
  <pre><code> 
    ./nifi-core/bin/nifi.sh start
    ./nifi-edge/bin/nifi.sh start
  </code></pre>
* Open the UI for both instances in a browser
  <pre><code> 
    http://localhost:7080/nifi/
    http://localhost:8080/nifi/
  </code></pre>

## Flink - WindowLogLevelCount - Setup

* Setup initial dictionary files
  <pre><code>
    mkdir nifi-edge/data
    mkdir nifi-edge/data/dictionary
    mkdir nifi-core/data
    mkdir nifi-core/data/dictionary
  </code></pre>
* In each of the above dictionary directories, create a file called levels.txt with the content:
<pre><code>
    ERROR
    WARN
</code></pre>

* Import nifi-flink-examples/src/main/resources/templates/nifi-flink-log-example-edge.xml into the the edge instance (http://localhost:7080/nifi)

* Import nifi-flink-examples/src/main/resources/templates/nifi-flink-log-example-core.xml into the the core instance (http://localhost:8080/nifi)

* Start everything on the core instance (http://localhost:8080/nifi)
![Image](https://github.com/bbende/nifi-streaming-examples/blob/master/nifi-flink-examples/screens/nifi-flink-core.tiff?raw=true)

* Run the Flink streaming job
<pre><code>
  cd nifi-flink-examples
  mvn clean package -PWindowLogLevelCount
  java -jar target/nifi-flink-examples-0.0.1-SNAPSHOT.jar
</code></pre>

* To start sending logs, starting everything on the edge instance (http://localhost:8080/nifi) EXCEPT the TailFile processor
![Image](https://github.com/bbende/nifi-streaming-examples/blob/master/nifi-flink-examples/screens/nifi-flink-edge.tiff?raw=true)
