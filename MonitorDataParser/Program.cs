using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace MonitorDataParser
{
    class Program
    {
        static void Main(string[] args)
        {
            bool f = true;

            Console.Title = "Monitor Header & Data Downloader - Itay Asayag";

            while (f)
            {
                Console.WriteLine("Select download type:");
                Console.WriteLine("  1. Headers");
                Console.WriteLine("  2. Binary Data");

                ConsoleKeyInfo cki = Console.ReadKey();

                switch (cki.Key)
                {
                    case ConsoleKey.D1:
                        DownloadHeaders();
                        f = false;
                        break;

                    case ConsoleKey.D2:
                        DownloadData();
                        f = false;
                        break;

                    default:
                        Console.WriteLine("(!) Invalid choice. Press any key...");
                        break;
                }

                Console.WriteLine("Program complete. Press any key...");
                Console.ReadKey();
            }
            
        }

        static void DownloadHeaders()
        {

            SqlConnection con = new SqlConnection(@"Server=BOBTOP\SQLEXPRESS;Database=NeoMonitor;Integrated Security=True;");

            con.Open();

            WebClient client1 = new WebClient();
            string[] names = client1.DownloadString($@"https://physionet.org/physiobank/database/ctu-uhb-ctgdb/RECORDS").Split('\n');

            foreach(string s in names)
            {
                int i = int.Parse(s);

                //string raw = System.IO.File.ReadAllText($@"D:\Projects\MonitorStartup\data\{i}.hea");
                Console.WriteLine($"Loading file {i}");
                WebClient client = new WebClient();
                string raw = client.DownloadString($@"http://physionet.org/physiobank/database/ctu-uhb-ctgdb/{i}.hea");
                Console.WriteLine($"Done. Parsing...");

                raw = raw.Replace('#', ' ').Replace(" ", "");

                string[] data = raw.Split('\n');

                int line = 7;
                float pH = float.Parse(data[line].Replace("pH", "").Replace("NaN", "-999")); line++;
                float BDecf = float.Parse(data[line].Replace("BDecf", "").Replace("NaN", "-999")); line++;
                float pCO2 = float.Parse(data[line].Replace("pCO2", "").Replace("NaN", "-999")); line++;
                float BE = float.Parse(data[line].Replace("BE", "").Replace("NaN", "-999")); line++;
                float Aggar1 = float.Parse(data[line].Replace("Apgar1", "").Replace("NaN", "-999")); line++;
                float Aggar2 = float.Parse(data[line].Replace("Apgar5", "").Replace("NaN", "-999")); line++;

                line = 23;
                float GestWeeks = float.Parse(data[line].Replace("Gest.weeks", "").Replace("NaN", "-999")); line++;
                float Weight = float.Parse(data[line].Replace("Weight(g)", "").Replace("NaN", "-999")); line++;
                float Sex = float.Parse(data[line].Replace("Sex", "").Replace("NaN", "-999")); line++;

                line = 28;
                float Age = float.Parse(data[line].Replace("Age", "").Replace("NaN", "-999")); line++;
                float Gravidity = float.Parse(data[line].Replace("Gravidity", "").Replace("NaN", "-999")); line++;
                float Parity = float.Parse(data[line].Replace("Parity", "").Replace("NaN", "-999")); line++;
                float Diabetes = float.Parse(data[line].Replace("Diabetes", "").Replace("NaN", "-999")); line++;
                float Hypertension = float.Parse(data[line].Replace("Hypertension", "").Replace("NaN", "-999")); line++;
                float Preeclampsia = float.Parse(data[line].Replace("Preeclampsia", "").Replace("NaN", "-999")); line++;
                float Liqpraecox = float.Parse(data[line].Replace("Liq.praecox", "").Replace("NaN", "-999")); line++;
                float Pyrexia = float.Parse(data[line].Replace("Pyrexia", "").Replace("NaN", "-999")); line++;
                float Meconium = float.Parse(data[line].Replace("Meconium", "").Replace("NaN", "-999")); line++;

                line = 39;
                float Presentation = float.Parse(data[line].Replace("Presentation", "").Replace("NaN", "-999")); line++;
                float Induced = float.Parse(data[line].Replace("Induced", "").Replace("NaN", "-999")); line++;
                float Istage = float.Parse(data[line].Replace("I.stage", "").Replace("NaN", "-999")); line++;
                float NoProgress = float.Parse(data[line].Replace("NoProgress", "").Replace("NaN", "-999")); line++;
                float CKKP = float.Parse(data[line].Replace("CK/KP", "").Replace("NaN", "-999")); line++;
                float IIstage = float.Parse(data[line].Replace("II.stage", "").Replace("NaN", "-999")); line++;
                float Delivtype = float.Parse(data[line].Replace("Deliv.type", "").Replace("NaN", "-999")); line++;

                line = 48;
                float dbID = float.Parse(data[line].Replace("dbID", "").Replace("NaN", "-999")); line++;
                float Rectype = float.Parse(data[line].Replace("Rec.type", "").Replace("NaN", "-999")); line++;
                float Pos2st = float.Parse(data[line].Replace("Pos.II.st.", "").Replace("NaN", "-999")); line++;
                float Sig2Birth = float.Parse(data[line].Replace("Sig2Birth", "").Replace("NaN", "-999")); line++;

                Console.WriteLine($"Done. Uploading...");


                SqlCommand cmd = new SqlCommand($@"
INSERT INTO [dbo].[tblHeaders]
           ([id]
           ,[pH]
           ,[BDecf]
           ,[pCO2]
           ,[BE]
           ,[Aggar1]
           ,[Aggar2]
           ,[GestWeeks]
           ,[Weight]
           ,[Sex]
           ,[Age]
           ,[Gravidity]
           ,[Parity]
           ,[Diabetes]
           ,[Hypertension]
           ,[Preeclampsia]
           ,[Liqpraecox]
           ,[Pyrexia]
           ,[Meconium]
           ,[Presentation]
           ,[Induced]
           ,[Istage]
           ,[NoProgress]
           ,[CKKP]
           ,[IIstage]
           ,[Delivtype]
           ,[dbID]
           ,[Rectype]
           ,[Pos2st]
           ,[Sig2Birth])
     VALUES
           ({i}
           ,{pH}
           ,{BDecf}
           ,{pCO2}
           ,{BE}
           ,{Aggar1}
           ,{Aggar2}
           ,{GestWeeks}
           ,{Weight}
           ,{Sex}
           ,{Age}
           ,{Gravidity}
           ,{Parity}
           ,{Diabetes}
           ,{Hypertension}
           ,{Preeclampsia}
           ,{Liqpraecox}
           ,{Pyrexia}
           ,{Meconium}
           ,{Presentation}
           ,{Induced}
           ,{Istage}
           ,{NoProgress}
           ,{CKKP}
           ,{IIstage}
           ,{Delivtype}
           ,{dbID}
           ,{Rectype}
           ,{Pos2st}
           ,{Sig2Birth})", con);

                cmd.ExecuteNonQuery();

                Console.WriteLine($"Done.");

            }
        }

        static void DownloadData()
        {
            SqlConnection con = new SqlConnection(@"Server=BOBTOP\SQLEXPRESS;Database=NeoMonitor;Integrated Security=True;");

            con.Open();

            WebClient client1 = new WebClient();
            Console.WriteLine("Downloading names list...");
            string[] names = client1.DownloadString($@"https://physionet.org/physiobank/database/ctu-uhb-ctgdb/RECORDS").Split('\n');
            Console.WriteLine("Done. Starting the monitor data download process.");

            foreach (string s in names)
            {
                int i = int.Parse(s);

                Console.WriteLine($"Starting to download file {i}.");
                byte[] response = new WebClient().DownloadData($"https://physionet.org/physiobank/database/ctu-uhb-ctgdb/{i}.dat");
                Console.WriteLine("Done. Uploading...");

                SqlCommand cmd = new SqlCommand($@"INSERT INTO [dbo].[tblData]
           ([id]
           ,[data])
     VALUES
           ({i}
           ,@response)", con);

                cmd.Parameters.AddWithValue("@response", response);
                cmd.ExecuteNonQuery();

                Console.WriteLine("Done.");
            }
        }
    }
}
