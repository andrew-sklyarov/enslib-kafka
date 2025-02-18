Class Production.Main Extends Ens.Production
{

XData ProductionDefinition
{
<Production Name="Production.Main" LogGeneralTraceEvents="true">
  <Description></Description>
  <ActorPoolSize>2</ActorPoolSize>
  <Setting Target="Adapter" Name="ShutdownTimeout">30</Setting>
  <Item Name="EnsLib.Kafka.Service" Category="" ClassName="EnsLib.Kafka.Service" PoolSize="1" Enabled="false" Foreground="false" Comment="" LogTraceEvents="true" Schedule="">
    <Setting Target="Host" Name="TargetConfigNames">VRM.Process.OnKafkaMessage</Setting>
    <Setting Target="Adapter" Name="CallInterval"></Setting>
    <Setting Target="Adapter" Name="GroupID">iris-sandbox-consumer-1</Setting>
    <Setting Target="Adapter" Name="Servers">kafka:29092</Setting>
    <Setting Target="Adapter" Name="Topic">my</Setting>
    <Setting Target="Adapter" Name="ReceiveSettings">{"pollTimeout": 100}</Setting>
  </Item>
  <Item Name="VRM.Process.OnKafkaMessage" Category="" ClassName="VRM.Process.OnKafkaMessage" PoolSize="1" Enabled="true" Foreground="false" Comment="" LogTraceEvents="true" Schedule="">
  </Item>
  <Item Name="VRM.Service.KafkaConsumer" Category="" ClassName="VRM.Service.KafkaConsumer" PoolSize="1" Enabled="false" Foreground="false" Comment="" LogTraceEvents="true" Schedule="">
    <Setting Target="Host" Name="Target">VRM.Process.OnKafkaMessage</Setting>
    <Setting Target="Host" Name="Servers">kafka:29092</Setting>
    <Setting Target="Host" Name="Topic">sample</Setting>
    <Setting Target="Adapter" Name="CallInterval">60</Setting>
    <Setting Target="Host" Name="GroupID"></Setting>
    <Setting Target="Host" Name="MaxChecksAtOnce">5</Setting>
  </Item>
</Production>
}

}
