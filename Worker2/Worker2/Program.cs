using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Cosmos.Table;

class Program
{
    static async Task Main()
    {
        Console.WriteLine("Verifying and sending alerts...");

        string storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=ambrosiaalertstorage;AccountKey=rwWTRwFJgtuSVOJikQMJUfyIFFKL172jcVYDS99AIHcO3KIFxNYaPKUAfCUqJqUfnNMwR+neA58a+ASt720Rzw==;EndpointSuffix=core.windows.net";
        string serviceBusConnectionString = "Endpoint=sb://alertsbus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=m1+yr5uKKBkZTx0KKwRD+c9rP85G7LnrQ+ASbPHCD6A=";
        string queueNameAlerts = "messages";

        CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);
        CloudTableClient tableClient = storageAccount.CreateCloudTableClient(new TableClientConfiguration());

        CloudTable usersTable = tableClient.GetTableReference("users");
        CloudTable zonesTable = tableClient.GetTableReference("ambrosiatable");

        ServiceBusClient client = new ServiceBusClient(serviceBusConnectionString);
        ServiceBusSender messageSender = client.CreateSender(queueNameAlerts);

        TableQuery<DynamicTableEntity> userQuery = new TableQuery<DynamicTableEntity>().Select(new List<string> { "PartitionKey","RowKey","Longitude","Latitude"});
        TableQuery<DynamicTableEntity> zonesQuery = new TableQuery<DynamicTableEntity>().Select(new List<string> { "PartitionKey","RowKey","longitude","latitude"});

        var users = usersTable.ExecuteQuery(userQuery);
        var zones = zonesTable.ExecuteQuery(zonesQuery);

        foreach (var user in users)
        {
            foreach (var zone in zones)
            {
                if (user.Properties.TryGetValue("Longitude", out var userLongitudeProperty) &&
                    user.Properties.TryGetValue("Latitude", out var userLatitudeProperty) &&
                    zone.Properties.TryGetValue("longitude", out var zoneLongitudeProperty) &&
                    zone.Properties.TryGetValue("latitude", out var zoneLatitudeProperty))
                {
                    double userLatitude = Convert.ToDouble(userLatitudeProperty.PropertyAsObject);
                    double userLongitude = Convert.ToDouble(userLongitudeProperty.PropertyAsObject);

                    double zoneLatitude = Convert.ToDouble(zoneLatitudeProperty.PropertyAsObject);
                    double zoneLongitude = Convert.ToDouble(zoneLongitudeProperty.PropertyAsObject);

                    double distance = CalculateHaversineDistance(userLatitude, userLongitude, zoneLatitude, zoneLongitude);
                    Console.WriteLine("Distance: " + distance);

                    if (distance < 500)
                    {
                        // Alert logic
                        string alertMessage = $"User {user.RowKey} is {distance} meters away from Ambrosia Zone {zone.RowKey}!";
                        Console.WriteLine(alertMessage);

                        ServiceBusMessage alert = new ServiceBusMessage(alertMessage);
                        await messageSender.SendMessageAsync(alert);
                    }
                }
            }
        }

        Console.WriteLine("Alert verification completed.");

        // Close resources
        await messageSender.CloseAsync();
        await client.DisposeAsync();
    }

    public static double CalculateHaversineDistance(double lat1, double lon1, double lat2, double lon2)
    {
        const double R = 6371; // Raza Pământului în kilometri

        var dLat = ToRadians(lat2 - lat1);
        var dLon = ToRadians(lon2 - lon1);

        var a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
                Math.Cos(ToRadians(lat1)) * Math.Cos(ToRadians(lat2)) *
                Math.Sin(dLon / 2) * Math.Sin(dLon / 2);

        var c = 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));

        var distance = R * c; // Distanta in kilometri

        return distance * 1000; // Converteste in metri
    }

    private static double ToRadians(double degree)
    {
        return degree * Math.PI / 180;
    }

}
