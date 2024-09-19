using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Microsoft.Extensions.Configuration;

class Program
{
    private static IConfiguration configuration;
    private static string inputDirectory;
    private static string referenceFilePath;
    private static string outputDirectory;
    private static string logDirectory;

    static void Main()
    {
        // Charger la configuration
        LoadConfiguration();
        // Créer le fichier de log
        string logFilePath = CreateLogFile();

        // Charger les MSISDNs depuis le fichier de référence
        HashSet<string> msisdns = new HashSet<string>(File.ReadLines(referenceFilePath));

        // Traiter les fichiers .gz
        ProcessFiles(msisdns, logFilePath);

        Console.WriteLine("Traitement terminé !");
    }

    private static void LoadConfiguration()
    {
        configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
            .Build();

        inputDirectory = configuration["Paths:InputDirectory"];
        referenceFilePath = configuration["Paths:ReferenceFile"];
        outputDirectory = configuration["Paths:OutputDirectory"];
        logDirectory = configuration["Paths:LogDirectory"];
    }

    private static string CreateLogFile()
    {
        // Assurer que le répertoire de log existe
        Directory.CreateDirectory(logDirectory);

        // Créer le nom du fichier de log avec un timestamp
        string logFileName = $"log_{DateTime.Now:yyyyMMdd_HHmmss}.txt";
        string logFilePath = Path.Combine(logDirectory, logFileName);
        return logFilePath;
    }

    private static void ProcessFiles(HashSet<string> msisdns, string logFilePath)
    {
        using (var logStream = new StreamWriter(logFilePath, append: true))
        {
            foreach (string filePath in Directory.GetFiles(inputDirectory, "*_gprsCall*.gz")
                                                  .Concat(Directory.GetFiles(inputDirectory, "*_mobileOriginatedCall*.gz")))
            {
                FileInfo fileInfo = new FileInfo(filePath);

                if (fileInfo.Length <= 1024)
                {
                    logStream.WriteLine($"Skipping file {filePath}: size {fileInfo.Length} bytes");
                    continue;
                }

                string outputFilePath = Path.Combine(outputDirectory, fileInfo.Name.Replace(".gz", ".txt"));
                bool foundMatchingLine = false;

                using (FileStream fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
                using (GZipStream gzipStream = new GZipStream(fileStream, CompressionMode.Decompress))
                using (StreamReader reader = new StreamReader(gzipStream))
                using (var outputStream = new StreamWriter(outputFilePath, append: false))
                {
                    // Lire et écrire l'en-tête
                    string headerLine = reader.ReadLine();
                    outputStream.WriteLine(headerLine); // Écrire l'en-tête dans le fichier de sortie

                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        string[] columns = line.Split('|');
                        if (columns.Length > 1)
                        {
                            string msisdn = columns[1].Trim();
                            if (msisdns.Contains(msisdn))
                            {
                                outputStream.WriteLine(line);
                                foundMatchingLine = true;
                            }
                        }
                    }
                }

                if (!foundMatchingLine)
                {
                    File.Delete(outputFilePath);
                    logStream.WriteLine($"No matches found for file {filePath}. Deleted output file {outputFilePath}.");
                }
                else
                {
                    logStream.WriteLine($"Processed file {filePath}. Output written to {outputFilePath}.");
                }
            }
        }
    }
}
