Class VRM.Process.OnKafkaMessage Extends Ens.BusinessProcess
{

/// Handle a message from Kafka
Method OnRequest(request As EnsLib.Kafka.Message, Output response As %Library.Persistent) As %Status
{
    q $$$OK
}

Storage Default
{
<Type>%Storage.Persistent</Type>
}

}
