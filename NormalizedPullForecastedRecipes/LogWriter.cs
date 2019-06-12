using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NormalizedPullForecastedRecipes
{
    class LogWriter
    { //Common method for writing log to the path
        private static void WriteLog(string msg)
        {
            DateTime dateTime = DateTime.Now.Date;

            string path = @"Application Log\";
            string fileName = dateTime.ToString("MM.dd.yyyy") + ".txt";

            if (!System.IO.File.Exists(path + fileName))
            {
                Directory.CreateDirectory(path);
                TextWriter tw = new StreamWriter(path + fileName);
                tw.WriteLine(DateTime.Now + " : " + msg);
                tw.Close();
            }
            else if (File.Exists(path + fileName))
            {
                using (var tw = new StreamWriter(path + fileName, true))
                {
                    tw.WriteLine(DateTime.Now + " : " + msg);
                }
            }
        }

        internal static void LogApplicationClosing()
        {
            WriteLog("----------------------------Application Closed---------------------");
        }

        internal static void LogApplicationRunning()
        {
            WriteLog("----------------------------Application Starts---------------------");
        }

        internal static void LogWithMessage(string msg)
        {
            WriteLog(msg);
        }
        internal static void WriteErrorLog(string msg)
        {
            DateTime dateTime = DateTime.Now;
            string path = ConfigurationManager.AppSettings["errorLogPath"];
            string fileName = dateTime.ToString("MM.dd.yyyy HH.mm.ss") + "NormalizedPullForecastedRecipes ERROR LOG.txt";
            if (String.IsNullOrEmpty(path))
            {
                WriteLog("Error Log Path specified incorrectly");
                return;
            }


            if (!System.IO.File.Exists(path + fileName))
            {
                Directory.CreateDirectory(path);
                TextWriter tw = new StreamWriter(path + fileName);
                tw.WriteLine(DateTime.Now + " : " + msg);
                tw.Close();
            }
            else if (File.Exists(path + fileName))
            {
                using (var tw = new StreamWriter(path + fileName, true))
                {
                    tw.WriteLine(DateTime.Now + " : " + msg);
                }
            }
        }
    }
}
