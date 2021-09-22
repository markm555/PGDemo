
using Npgsql;
using System;
using System.Net;
using System.Data;
using Newtonsoft.Json.Linq;
using System.Linq;

namespace PGDemo
{
    class Program
    {
        public class Flightrec
        {
            public string flight { get; set; }          // Flight Number as filed
            public double baro_rate { get; set; }       // Barometric rate of change of altitude in feet/minute
            public string nav_modes { get; set; }       // Navigation mode such as autopilot
            public double gs { get; set; }              // Ground speed
            public double lat { get; set; }             // Latitude of current position
            public double lon { get; set; }             // Longitude of current position
        }

        public static class StaticItems
        /*
         * Set up endpoint to Dump 1090 on Raspberry Pi to pull Json
         */
        {
            public static string EndPoint = "http://<ipaddress of piaware>/dump1090-fa/data/aircraft.json";
        }
        static void Main(string[] args)
        {
            var webClient = new WebClient();
            webClient.BaseAddress = StaticItems.EndPoint;
            var cs = "Server=postgres-demo.postgres.database.azure.com;Database=<Database>;Port=5432;User Id=<UserName>;Password=<Password>;Ssl Mode=Require";

            using var con = new NpgsqlConnection(cs);
            con.Open();

            Console.Clear();  //clear the console

            while (true)
            {
                try
                {
                    var json = webClient.DownloadString("aircraft.json");

                    JToken token = JToken.Parse(json);  // Parse JSON from Rasperry Pi.
                    JArray aircraft = (JArray)token.SelectToken("aircraft");  // Pull out just the flight records from the JSON.
                    JArray saircraft = new JArray(aircraft.OrderBy(obj => (string)obj["flight"])); //Create a new array and sort records by flight number.

                    var i = 0;

                    // Only process records with the hex,flight,lat,lon,alt_baro,baro_rate,track and gs columns.
                    foreach (JToken ac in saircraft)
                    {
                        if (ac["hex"] != null &
                            ac["flight"] != null &
                            ac["lat"] != null &
                            ac["lon"] != null &
                            ac["alt_baro"] != null &
                            ac["baro_rate"] != null&
                            ac["gs"] != null)

                        {
                            i++;

                        // set variables with the contents of record in JArray.  One record is written to the databases and console for each pass of the loop

                        DateTime datim = DateTime.Now;
                        string flight = Convert.ToString(ac["flight"]);
                        double baro_rate = Convert.ToDouble(ac["baro_rate"]);
                        string nav_modes = Convert.ToString(ac["nav_modes"]);
                        double lat = Convert.ToDouble(ac["lat"]);
                        double lon = Convert.ToDouble(ac["lon"]);
                        double gs = Convert.ToDouble(ac["gs"]);



                        var sql = "INSERT INTO KDFW(pdt, pflight, pbaro_rate, pnav_mode, plat, plon, gs) VALUES(@pdt, @pflight, @pbaro_rate, @pnav_mode, @plat, @plon, @gs)";
                        using var cmd = new NpgsqlCommand(sql, con);

                        cmd.Parameters.AddWithValue("pdt", datim);
                        cmd.Parameters.AddWithValue("pflight", flight);
                        cmd.Parameters.AddWithValue("pbaro_rate", baro_rate);
                        cmd.Parameters.AddWithValue("pnav_mode", nav_modes);
                        cmd.Parameters.AddWithValue("plat", lat);
                        cmd.Parameters.AddWithValue("plon", lon);
                        cmd.Parameters.AddWithValue("gs", gs);
                        cmd.Prepare();

                        cmd.ExecuteNonQuery();

                        //Console.WriteLine("Flight: ", flight, " ", lat, " ", lon);
                        Console.WriteLine(flight + " | " + lat + " | " + lon + " | " + gs);
                    }
                    }
                }
                catch 
                {
                    Console.WriteLine(DateTime.Now + ": " + "Connection timeout retrying...");
                    try
                    {
                        con.Close();
                        con.Open();
                    }
                    catch { };
                }
            }
        }
    }
}
