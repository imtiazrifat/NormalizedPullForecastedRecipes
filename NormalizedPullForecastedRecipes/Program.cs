using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NormalizedPullForecastedRecipes
{
    class Program
    {
        static void Main(string[] args)
        {
            LogWriter.LogApplicationRunning();
            NormalizedPullForecastedRecipes();
            LogWriter.LogApplicationClosing();
        }

        /// <summary>
        /// NormalizedPullForecastedRecipes() will call each time the application runs
        /// </summary>
        private static void NormalizedPullForecastedRecipes()
        {
            LogWriter.LogWithMessage("NormalizedPullForecastedRecipes: " + DateTime.Now);
            DataLayer.NormalizedPullForecastedRecipesMaster();
        }
    }
}
