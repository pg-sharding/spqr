<?xml version="1.0"?>
<parameters>
    <type>SPQR</type>
    <driver>org.postgresql.Driver</driver>
    <!-- Router Url -->
    <url>jdbc:postgresql://${router_ips[0]}:6432/db1?sslmode=disable&amp;preferQueryMode=simple</url>
    <username>user1</username>
    <password>password</password>
    <preferQueryMode>simple</preferQueryMode>
    <reconnectOnConnectionFailure>true</reconnectOnConnectionFailure>
    <isolation>TRANSACTION_READ_COMMITTED</isolation>
    <batchsize>1024</batchsize>

    <shardUrls>
%{ for ip in shard_ips ~}
        <shardUrl>jdbc:postgresql://${ip}:6432/db1?sslmode=require</shardUrl>
%{ endfor }
    </shardUrls>

    <!-- Upper limits are the highest warehouse id (up to and including) that is stored at corresponding shard-->
    <upperLimits>
%{ for i, ip in shard_ips ~}
        <upperLimit>${(i+1)*250 - 1}</upperLimit>
%{ endfor }
    </upperLimits>
    <!-- The workload -->
    <terminals>2048</terminals>
    <works>
        <work>
            <warmup>300</warmup>
            <time>3000</time>
            <rate>unlimited</rate>
            <weights>45,43,4,4,4</weights>
        </work>
    </works>

    <!-- TPCC specific -->
    <transactiontypes>
        <transactiontype>
            <name>NewOrder</name>
        </transactiontype>
        <transactiontype>
            <name>Payment</name>
        </transactiontype>
        <transactiontype>
            <name>OrderStatus</name>
        </transactiontype>
        <transactiontype>
            <name>Delivery</name>
        </transactiontype>
        <transactiontype>
            <name>StockLevel</name>
        </transactiontype>
    </transactiontypes>
</parameters>