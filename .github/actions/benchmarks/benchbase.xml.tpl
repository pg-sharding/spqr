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
    <isolation>TRANSACTION_SERIALIZABLE</isolation>
    <batchsize>128</batchsize>

    <scalefactor>1</scalefactor>

    <shardUrls>
%{ for ip in shard_ips ~}
        <shardUrl>jdbc:postgresql://${ip}:6432/db1?sslmode=require</shardUrl>
%{ endfor }
    </shardUrls>

    <!-- Upper limits are the highest warehouse id (up to and including) that is stored at corresponding shard-->
    <upperLimits>
%{ for i, ip in shard_ips ~}
        <upperLimit>${(i+1)*2}</upperLimit>
%{ endfor }
    </upperLimits>
    <!-- The workload -->
    <terminals>2</terminals>
    <works>
        <work>
            <warmup>30</warmup>
            <time>300</time>
            <rate>unlimited</rate>
            <weights>45,43,4,4,4</weights>
        </work>
    </works>

    <!-- TPCC specific -->
    <transactiontypes>
        <transactiontype>
            <name>NewOrder</name>
            <preExecutionWait>180</preExecutionWait>
            <postExecutionWait>120</postExecutionWait>
        </transactiontype>
        <transactiontype>
            <name>Payment</name>
            <preExecutionWait>30</preExecutionWait>
            <postExecutionWait>120</postExecutionWait>
        </transactiontype>
        <transactiontype>
            <name>OrderStatus</name>
            <preExecutionWait>20</preExecutionWait>
            <postExecutionWait>100</postExecutionWait>
        </transactiontype>
        <transactiontype>
            <name>Delivery</name>
            <preExecutionWait>20</preExecutionWait>
            <postExecutionWait>50</postExecutionWait>
        </transactiontype>
        <transactiontype>
            <name>StockLevel</name>
            <preExecutionWait>20</preExecutionWait>
            <postExecutionWait>50</postExecutionWait>
        </transactiontype>
    </transactiontypes>
</parameters>