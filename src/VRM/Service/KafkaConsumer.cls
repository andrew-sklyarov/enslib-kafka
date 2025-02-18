/// Consumes Protobuf messages from Kafka
Class VRM.Service.KafkaConsumer Extends (Ens.BusinessService, EnsLib.Kafka.Common, EnsLib.Messaging.Common)
{

/// Target to send messages to
Property Target As %String;

/// Topic that the messages are read from
Property Topic As %String(MAXLEN = 100);

/// Kafka consumer group ID. Within the same group, each message is only read once
Property GroupID As %String(MAXLEN = 100);

/// Max attempts ReceiveMessage call at once
Property MaxChecksAtOnce As %Integer [ InitialExpression = 2 ];

/// Set dependencies folder in ExtraClasspaths
Property ValueDeserializerClass As %String(MAXLEN = 100) [ InitialExpression = "org.apache.kafka.common.serialization.StringDeserializer" ];

Property Client As %External.Messaging.KafkaClient;

Parameter ADAPTER = "Ens.InboundAdapter";

Parameter SETTINGS = "Topic:Kafka Settings,GroupID:Kafka Settings,MaxChecksAtOnce:Kafka Settings,ValueDeserializerClass:Kafka Settings,Target:Basic:selector?context={Ens.ContextSearch/ProductionItems?targets=1&productionName=@productionId}";

Method InitKafkaClient() As %Status
{
    s settings = ##class(%External.Messaging.KafkaSettings).%New()
    
    s settings.servers = ..Servers
    s settings.groupId = ..GroupID
    s settings.securityprotocol = ..SecurityProtocol
    s settings.saslmechanism = ..SASLMechanism
    s settings.truststorelocation = ..TrustStoreLocation
    s settings.keystorelocation = ..KeyStoreLocation

    // Credentials setup
    s creds = ..getCredentials(..Credentials)
    s settings.username = $li(creds, 1)
    s settings.password = $li(creds, 2)
    s settings.truststorepassword = $li(..getCredentials(..TrustStoreCredentials), 2)
    s settings.keystorepassword = $li(..getCredentials(..KeyStoreCredentials), 2)
    s settings.keypassword = $li(..getCredentials(..KeyCredentials), 2)

    s ..Client = ##class(%External.Messaging.Client).CreateClient(settings, .tSC, ..ExtraClasspaths)
    q:$$$ISERR(tSC) tSC

    s consumerSettings = {"value.deserializer": (..ValueDeserializerClass)}
    
    s tSC = ..Client.UpdateConsumerConfig(consumerSettings.%ToJSON())
    q:$$$ISERR(tSC) tSC

    $$$TRACE("Kafka client initialized successfully")
    $$$TRACE($$$FormatText("Client settings: %1", settings.ToJSON()))
    $$$TRACE($$$FormatText("Consumer settings: %1", consumerSettings.%ToJSON()))

    q tSC
}

Method CloseKafkaClient() As %Status
{
    $$$TRACE("Close Kafka client")
    s tSC = $s(..Client=$$$NULLOREF:$$$OK, 1:..Client.Close())
    q tSC
}

Method ConsumeMessages() As %Status
{
    #dim messages As %ListOfObjects
    s counter = 0

    while 1 {
        s counter = $i(counter)
		s tSC = ..Client.ReceiveMessage(..Topic, .messages)
        
        q:$$$ISERR(tSC)
        q:messages.Size>0
        q:counter>=..MaxChecksAtOnce
    }
    q:$$$ISERR(tSC) tSC

    $$$TRACE($$$FormatText("Received %1 messages from topic '%2'", messages.Size, ..Topic))

    f i=1:1:messages.Size {
        #dim msg As %External.Messaging.KafkaMessage = messages.GetAt(i)
        
        s request = ##class(EnsLib.Kafka.Message).%New()
        s request.value = msg.value

        s tSC = ..SendRequestAsync(..Target, request)
		q:$$$ISERR(tSC)
    }

    q tSC
}

Method OnInit() As %Status
{
	q ..InitKafkaClient()
}

Method OnTearDown() As %Status
{
    q ..CloseKafkaClient()
}

Method OnProcessInput(pInput As %RegisteredObject, Output pOutput As %RegisteredObject, ByRef pHint As %String) As %Status
{
    s tSC = $s(..Client=$$$NULLOREF:$$$OK, 1:..ConsumeMessages())
    q tSC
}

ClassMethod OnGetConnections(Output pArray As %String, pItem As Ens.Config.Item)
{
	s:pItem.GetModifiedSetting("Target", .tValue) pArray(tValue) = ""
}

}
