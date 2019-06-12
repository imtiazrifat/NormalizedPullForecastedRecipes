using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NormalizedPullForecastedRecipes
{
    class DataLayer
    {
        #region Initiating syncing
        /// <summary>
        /// Primary method from where all the execution will take place
        /// </summary>
        internal static void NormalizedPullForecastedRecipesMaster()
        {
            //Get all current data from Food Pro database
            // If any of these method faild to execute then the application with terminate
            if (!SyncAllMealName())
            {
                LogWriter.LogWithMessage("ERROR! from SyncAllMealName.");
                return;
            }
            if (!SyncAllMealLocation())
            {
                LogWriter.LogWithMessage("ERROR! from SyncAllMealLocation.");
                return;
            }
            if (!SyncAllMenuCategory())
            {
                LogWriter.LogWithMessage("ERROR! from SyncAllMenuCategory.");
                return;
            }
            if (!SyncAllRecipeExtension())
            {
                LogWriter.LogWithMessage("ERROR! from SyncAllRecipeExtension.");
                return;
            }
            if (!SyncAllRecipesWarehouse())
            {
                LogWriter.LogWithMessage("ERROR! from SyncAllRecipesWarehouse.");
                return;
            }
        }
        #endregion


        #region syncing process 
        /// <summary>
        /// Sync All Meal Names, comparing with existing with the new ones and insert those that do not exist in our table. All checking is done by SQL
        /// </summary>
        /// <returns>true for successfull execution </returns>
        private static bool SyncAllMealName()
        {
            try
            {

                var con = ConfigurationManager.ConnectionStrings["ConnectionDiningAnalytics"].ToString();
                using (SqlConnection myConnection = new SqlConnection(con))


                using (var cmd = myConnection.CreateCommand())
                {
                    //The query first selects all the new MealName, compare with existing MealName. Selects only those meal name(s) that does not exist in the database and insert those
                    string qry = String.Format(@"INSERT INTO [warehouse].[MealName]
                                                           ([MealName]
                                                           ,[MealNumber]
                                                           ,[Note]
		                                                   ,[CreatedDate])

                                                select tblMeal.Meal_Name,tblMeal.Meal_Number,'System Generated' Note, GETDATE() CreatedDate from [warehouse].[MealName] WM
                                                right  join ( select Meal_Number,Meal_Name from [Landinig].[ForecastedRecipes] group by  Meal_Number,Meal_Name ) tblMeal
                                                ON WM.[MealName] = tblMeal.Meal_Name and  WM.[MealNumber] = tblMeal.Meal_Number Where MealNameId is null");

                    myConnection.Open();
                    cmd.CommandText = qry;
                    var rowsAffected = cmd.ExecuteNonQuery();
                    LogWriter.LogWithMessage("SyncAllMealName Executed. Number of data inserted : " + rowsAffected);
                    return true;
                }
            }
            catch (Exception ex)
            {
                LogWriter.WriteErrorLog("EXCEPTION from SyncAllMealName: " + ex.Message);
                LogWriter.LogWithMessage("EXCEPTION from SyncAllMealName: " + ex.Message);
                return false;
            }
        }


        /// <summary>
        /// Sync All Location, comparing with existing with the new ones and insert those that do not exist in our table. All checking is done by SQL
        /// </summary>
        /// <returns>true for successfull execution </returns>
        private static bool SyncAllMealLocation()
        {
            try
            {

                var con = ConfigurationManager.ConnectionStrings["ConnectionDiningAnalytics"].ToString();
                using (SqlConnection myConnection = new SqlConnection(con))


                using (var cmd = myConnection.CreateCommand())
                {
                    //The query first selects all the new Location, compare with existing Location. Selects only those Location that does not exist in the database and insert those
                    string qry = String.Format(@"INSERT INTO [warehouse].[Location]
                                                   ([LocationName]
                                                   ,[LocationNumber]
                                                   ,[Note]
                                                   ,[CreatedDate])
                                        select Location_Name,Location_Number,'System Generated' Note, GETDATE() CreatedDate from [warehouse].[Location] WL
                                        right  join ( 
                                        select Location_Name,Location_Number from [Landinig].[ForecastedRecipes] group by  Location_Name,Location_Number ) tblLocation
                                        ON WL.[LocationName] = tblLocation.Location_Name and  WL.[LocationNumber] = tblLocation.Location_Number
                                        Where LocationId is null");

                    myConnection.Open();
                    cmd.CommandText = qry;
                    var rowsAffected = cmd.ExecuteNonQuery();
                    LogWriter.LogWithMessage("SyncAllMealLocation Executed. Number of data inserted : " + rowsAffected);
                    return true;
                }
            }
            catch (Exception ex)
            {
                LogWriter.WriteErrorLog("EXCEPTION from SyncAllMealLocation: " + ex.Message);
                LogWriter.LogWithMessage("EXCEPTION from SyncAllMealLocation: " + ex.Message);
                return false;
            }
        }


        /// <summary>
        /// Sync All Menu Category, comparing with existing with the new ones and insert those that do not exist in our table. All checking is done by SQL
        /// </summary>
        /// <returns>true for successfull execution </returns>
        private static bool SyncAllMenuCategory()
        {
            try
            {

                var con = ConfigurationManager.ConnectionStrings["ConnectionDiningAnalytics"].ToString();
                using (SqlConnection myConnection = new SqlConnection(con))


                using (var cmd = myConnection.CreateCommand())
                {
                    //The query first selects all the new MenuCategories, compare with existing MenuCategories. Selects only those MenuCategories that does not exist in the database and insert those
                    string qry = String.Format(@"INSERT INTO [warehouse].[MenuCategories]
                                                   ([MenuCategoryName]
                                                   ,[MenuCategoryNumber]
                                                   ,[Note]
                                                   ,[CreatedDate])
                                        select Menu_Category_Name,Menu_Category_Number,'System Generated' Note, GETDATE() CreatedDate from [warehouse].[MenuCategories] WMC
                                        right  join ( 
                                        select Menu_Category_Name,Menu_Category_Number from [Landinig].[ForecastedRecipes] group by  Menu_Category_Name,Menu_Category_Number ) tblCategory
                                        ON WMC.[MenuCategoryName] = tblCategory.Menu_Category_Name and  WMC.[MenuCategoryNumber] = tblCategory.Menu_Category_Number
                                        Where [MenuCategoryId] is null");

                    myConnection.Open();
                    cmd.CommandText = qry;
                    var rowsAffected = cmd.ExecuteNonQuery();
                    LogWriter.LogWithMessage("SyncAllMenuCategory Executed. Number of data inserted : " + rowsAffected);
                    return true;
                }
            }
            catch (Exception ex)
            {
                LogWriter.WriteErrorLog("EXCEPTION from SyncAllMenuCategory: " + ex.Message);
                LogWriter.LogWithMessage("EXCEPTION from SyncAllMenuCategory: " + ex.Message);
                return false;
            }
        }


        /// <summary>
        /// Sync All Recipe Extension, comparing with existing with the new ones and insert those that do not exist in our table. All checking is done by SQL
        /// </summary>
        /// <returns>true for successfull execution </returns>
        private static bool SyncAllRecipeExtension()
        {
            try
            {

                var con = ConfigurationManager.ConnectionStrings["ConnectionDiningAnalytics"].ToString();
                using (SqlConnection myConnection = new SqlConnection(con))


                using (var cmd = myConnection.CreateCommand())
                {
                    //The query first selects all the new RecipeExtension, compare with existing RecipeExtension. Selects only those RecipeExtension that does not exist in the database and insert those
                    string qry = String.Format(@"INSERT INTO [warehouse].[RecipeExtension]
                                                        ([Recipe_Name]
                                                        ,[Recipe_Number]
                                                        ,[Recipe_Print_As_Name]
                                                        ,[Ingredient_List]
                                                        ,[Allergens]
                                                        ,[Recipe_Print_As_Color]
                                                        ,[Recipe_Print_As_Character]
                                                        ,[Recipe_Product_Information]
                                                        ,[Selling_Price]
                                                        ,[Portion_Cost]
                                                        ,[Production_Department]
                                                        ,[Service_Department]
                                                        ,[Catering_Department]
                                                        ,[Recipe_Web_Codes]
                                                        ,[Serving_Size]
                                                        ,[Calories]
                                                        ,[Calories_From_Fat]
                                                        ,[Total_Fat]
                                                        ,[Total_Fat_DV]
                                                        ,[Sat_Fat]
                                                        ,[Sat_Fat_DV]
                                                        ,[Trans_Fat]
                                                        ,[Trans_Fat_DV]
                                                        ,[Cholesterol]
                                                        ,[Cholesterol_DV]
                                                        ,[Sodium]
                                                        ,[Sodium_DV]
                                                        ,[Total_Carb]
                                                        ,[Total_Carb_DV]
                                                        ,[Dietary_Fiber]
                                                        ,[Dietary_Fiber_DV]
                                                        ,[Sugars]
                                                        ,[Sugars_DV]
                                                        ,[Protein]
                                                        ,[Protein_DV]           
                                                        ,[Note]
                                                        ,[CollectionDateTime])
                                                        select 
                                                        tblNWRecipeExtension.[Recipe_Name]
                                                        ,tblNWRecipeExtension.[Recipe_Number]
                                                        ,tblNWRecipeExtension.[Recipe_Print_As_Name]
                                                        ,tblNWRecipeExtension.[Ingredient_List]
                                                        ,tblNWRecipeExtension.[Allergens]
                                                        ,tblNWRecipeExtension.[Recipe_Print_As_Color]
                                                        ,tblNWRecipeExtension.[Recipe_Print_As_Character]
                                                        ,tblNWRecipeExtension.[Recipe_Product_Information]
                                                        ,tblNWRecipeExtension.[Selling_Price]
                                                        ,tblNWRecipeExtension.[Portion_Cost]
                                                        ,tblNWRecipeExtension.[Production_Department]
                                                        ,tblNWRecipeExtension.[Service_Department]
                                                        ,tblNWRecipeExtension.[Catering_Department]
                                                        ,tblNWRecipeExtension.[Recipe_Web_Codes]
                                                        ,tblNWRecipeExtension.[Serving_Size]
                                                        ,tblNWRecipeExtension.[Calories]
                                                        ,tblNWRecipeExtension.[Calories_From_Fat]
                                                        ,tblNWRecipeExtension.[Total_Fat]
                                                        ,tblNWRecipeExtension.[Total_Fat_DV]
                                                        ,tblNWRecipeExtension.[Sat_Fat]
                                                        ,tblNWRecipeExtension.[Sat_Fat_DV]
                                                        ,tblNWRecipeExtension.[Trans_Fat]
                                                        ,tblNWRecipeExtension.[Trans_Fat_DV]
                                                        ,tblNWRecipeExtension.[Cholesterol]
                                                        ,tblNWRecipeExtension.[Cholesterol_DV]
                                                        ,tblNWRecipeExtension.[Sodium]
                                                        ,tblNWRecipeExtension.[Sodium_DV]
                                                        ,tblNWRecipeExtension.[Total_Carb]
                                                        ,tblNWRecipeExtension.[Total_Carb_DV]
                                                        ,tblNWRecipeExtension.[Dietary_Fiber]
                                                        ,tblNWRecipeExtension.[Dietary_Fiber_DV]
                                                        ,tblNWRecipeExtension.[Sugars]
                                                        ,tblNWRecipeExtension.[Sugars_DV]
                                                        ,tblNWRecipeExtension.[Protein]
                                                        ,tblNWRecipeExtension.[Protein_DV]
                                                        ,'System Generated' Note, GETDATE() CreatedDate from [warehouse].RecipeExtension WRE
                                                                            right  join (
                                                        select 
                                                        [Recipe_Name]
                                                        ,[Recipe_Number]
                                                        ,[Recipe_Print_As_Name]
                                                        ,CAST( [Ingredient_List] AS NVARCHAR(max)) [Ingredient_List]
                                                        ,CAST( [Allergens] AS NVARCHAR(max)) [Allergens]
                                                        ,[Recipe_Print_As_Color]
                                                        ,[Recipe_Print_As_Character]
                                                        ,CAST( [Recipe_Product_Information] AS NVARCHAR(max)) [Recipe_Product_Information]
                                                        ,[Selling_Price]
                                                        ,[Portion_Cost]
                                                        ,[Production_Department]
                                                        ,[Service_Department]
                                                        ,[Catering_Department]
                                                        ,[Recipe_Web_Codes]
                                                        ,[Serving_Size]
                                                        ,[Calories]
                                                        ,[Calories_From_Fat]
                                                        ,[Total_Fat]
                                                        ,[Total_Fat_DV]
                                                        ,[Sat_Fat]
                                                        ,[Sat_Fat_DV]
                                                        ,[Trans_Fat]
                                                        ,[Trans_Fat_DV]
                                                        ,[Cholesterol]
                                                        ,[Cholesterol_DV]
                                                        ,[Sodium]
                                                        ,[Sodium_DV]
                                                        ,[Total_Carb]
                                                        ,[Total_Carb_DV]
                                                        ,[Dietary_Fiber]
                                                        ,[Dietary_Fiber_DV]
                                                        ,[Sugars]
                                                        ,[Sugars_DV]
                                                        ,[Protein]
                                                        ,[Protein_DV]
                                                        from [Landinig].[ForecastedRecipes] group by  
                                                        [Recipe_Name]
                                                        ,[Recipe_Number]
                                                        ,[Recipe_Print_As_Name]
                                                        , CAST( [Ingredient_List] AS NVARCHAR(max))
                                                        , CAST( [Allergens] AS NVARCHAR(max))
                                                        ,[Recipe_Print_As_Color]
                                                        ,[Recipe_Print_As_Character]
                                                        ,CAST( [Recipe_Product_Information] AS NVARCHAR(max))
                                                        ,[Selling_Price]
                                                        ,[Portion_Cost]
                                                        ,[Production_Department]
                                                        ,[Service_Department]
                                                        ,[Catering_Department]
                                                        ,[Recipe_Web_Codes]
                                                        ,[Serving_Size]
                                                        ,[Calories]
                                                        ,[Calories_From_Fat]
                                                        ,[Total_Fat]
                                                        ,[Total_Fat_DV]
                                                        ,[Sat_Fat]
                                                        ,[Sat_Fat_DV]
                                                        ,[Trans_Fat]
                                                        ,[Trans_Fat_DV]
                                                        ,[Cholesterol]
                                                        ,[Cholesterol_DV]
                                                        ,[Sodium]
                                                        ,[Sodium_DV]
                                                        ,[Total_Carb]
                                                        ,[Total_Carb_DV]
                                                        ,[Dietary_Fiber]
                                                        ,[Dietary_Fiber_DV]
                                                        ,[Sugars]
                                                        ,[Sugars_DV]
                                                        ,[Protein]
                                                        ,[Protein_DV] ) tblNWRecipeExtension

                                                        on WRE.[Recipe_Name] = tblNWRecipeExtension.[Recipe_Name]  and WRE.[Recipe_Number] = tblNWRecipeExtension.[Recipe_Number] 
                                                        and WRE.[Recipe_Print_As_Name] = tblNWRecipeExtension.[Recipe_Print_As_Name]  and CAST( WRE.[Ingredient_List] AS NVARCHAR(max)) = tblNWRecipeExtension.[Ingredient_List]
                                                        and CAST( WRE.[Allergens] AS NVARCHAR(max)) = tblNWRecipeExtension.[Allergens]  and WRE.[Recipe_Print_As_Color] = tblNWRecipeExtension.[Recipe_Print_As_Color]
                                                        and WRE.[Recipe_Print_As_Character] = tblNWRecipeExtension.[Recipe_Print_As_Character]  and CAST( WRE.[Recipe_Product_Information] AS NVARCHAR(max)) = tblNWRecipeExtension.[Recipe_Product_Information]
                                                        and WRE.[Selling_Price] = tblNWRecipeExtension.[Selling_Price]  and WRE.[Portion_Cost] = tblNWRecipeExtension.[Portion_Cost]
                                                        and WRE.[Production_Department] = tblNWRecipeExtension.[Production_Department]  and WRE.[Service_Department] = tblNWRecipeExtension.[Service_Department]
                                                        and WRE.[Catering_Department] = tblNWRecipeExtension.[Catering_Department]  and WRE.[Recipe_Web_Codes] = tblNWRecipeExtension.[Recipe_Web_Codes]
                                                        and WRE.[Serving_Size] = tblNWRecipeExtension.[Serving_Size]  and WRE.[Calories] = tblNWRecipeExtension.[Calories]
                                                        and WRE.[Calories_From_Fat] = tblNWRecipeExtension.[Calories_From_Fat]  and WRE.[Total_Fat] = tblNWRecipeExtension.[Total_Fat]
                                                        and WRE.[Total_Fat_DV] = tblNWRecipeExtension.[Total_Fat_DV]  and WRE.[Sat_Fat] = tblNWRecipeExtension.[Sat_Fat]  
                                                        and WRE.[Sat_Fat_DV] = tblNWRecipeExtension.[Sat_Fat_DV]  and WRE.[Trans_Fat] = tblNWRecipeExtension.[Trans_Fat]	   
                                                        and WRE.[Trans_Fat_DV] = tblNWRecipeExtension.[Trans_Fat_DV] and WRE.[Cholesterol] = tblNWRecipeExtension.[Cholesterol]
                                                        and WRE.[Cholesterol_DV] = tblNWRecipeExtension.[Cholesterol_DV]  and WRE.[Sodium] = tblNWRecipeExtension.[Sodium]
                                                        and WRE.[Sodium_DV] = tblNWRecipeExtension.[Sodium_DV]  and WRE.[Total_Carb] = tblNWRecipeExtension.[Total_Carb]
                                                        and WRE.[Dietary_Fiber] = tblNWRecipeExtension.[Dietary_Fiber] and WRE.[Total_Carb_DV] = tblNWRecipeExtension.[Total_Carb_DV]
                                                        and WRE.[Dietary_Fiber_DV] = tblNWRecipeExtension.[Dietary_Fiber_DV]  and WRE.[Sugars] = tblNWRecipeExtension.[Sugars]
                                                        and WRE.[Sugars_DV] = tblNWRecipeExtension.[Sugars_DV]  and WRE.[Protein] = tblNWRecipeExtension.[Protein]
                                                        and WRE.[Protein_DV] = tblNWRecipeExtension.[Protein_DV]
                                                        Where RecipeId is null");

                    myConnection.Open();
                    cmd.CommandText = qry;
                    var rowsAffected = cmd.ExecuteNonQuery();
                    LogWriter.LogWithMessage("SyncAllRecipeExtension Executed. Number of data inserted : " + rowsAffected);
                    return true;
                }
            }
            catch (Exception ex)
            {
                LogWriter.WriteErrorLog("EXCEPTION from SyncAllRecipeExtension: " + ex.Message);
                LogWriter.LogWithMessage("EXCEPTION from SyncAllRecipeExtension: " + ex.Message);
                return false;
            }
        }

        /// <summary>
        /// Sync All Recipes Warehouse, comparing with existing with the new ones and insert those that do not exist in our table. 
        /// All checking is done by SQL. Here the master table information is managed via joining and getting the primary key against the information of the whole table
        /// </summary>
        /// <returns>true for successfull execution </returns>
        private static bool SyncAllRecipesWarehouse()
        {
            try
            {

                var con = ConfigurationManager.ConnectionStrings["ConnectionDiningAnalytics"].ToString();
                using (SqlConnection myConnection = new SqlConnection(con))


                using (var cmd = myConnection.CreateCommand())
                {
                    string qry = String.Format(@"
INSERT INTO [warehouse].[RecipesWarehouse]
           ([TheirId]
           ,[Serve_Date]
           ,[LocationId]
           ,[MealNameId]
           ,[MenuCategoryId]
           ,RecipeId
		   ,CreatedDate)
            Select  tbl.ID,tbl.Serve_Date,tbl.LocationId, tbl.MealNameId,tbl.MenuCategoryId, tbl.RecipeId, GETDATE() CreatedDate  from (
            Select FR.ID,Serve_Date,WL.LocationId, WML.MealNameId,WMC.MenuCategoryId, WRE.RecipeId
            from [Landinig].[ForecastedRecipes] FR
            inner join [warehouse].[MealName] WML on WML.MealName = FR.Meal_name and WML.MealNumber = FR.Meal_Number 
            inner join [warehouse].[Location] WL on WL.LocationName = FR.Location_Name and  WL.LocationNumber = FR.Location_Number
            inner join [warehouse].[MenuCategories] WMC on WMC.MenuCategoryName = FR.Menu_Category_Name and WMC.MenuCategoryNumber = FR.Menu_Category_Number
            inner join [warehouse].[RecipeExtension] WRE on 
            WRE.[Recipe_Name] = FR.[Recipe_Name] and
            WRE.[Recipe_Number] = FR.[Recipe_Number] and 
            WRE.[Recipe_Print_As_Name] = FR.[Recipe_Print_As_Name] and 
            CAST( WRE.[Ingredient_List] AS NVARCHAR(max))=  CAST(FR.[Ingredient_List] AS NVARCHAR(max)) and 
            CAST( WRE.[Allergens] AS NVARCHAR(max)) =  CAST( FR.[Allergens] AS NVARCHAR(max)) and 
            WRE.[Recipe_Print_As_Color] = FR.[Recipe_Print_As_Color] and 
            WRE.[Recipe_Print_As_Character] = FR.[Recipe_Print_As_Character] and 
            CAST(  WRE.[Recipe_Product_Information] AS NVARCHAR(max)) = CAST(  FR.[Recipe_Product_Information]  AS NVARCHAR(max)) and 
            WRE.[Selling_Price] = FR.[Selling_Price] and 
            WRE.[Portion_Cost] = FR.[Portion_Cost] and 
            WRE.[Production_Department] = FR.[Production_Department] and 
            WRE.[Service_Department] = FR.[Service_Department] and 
            WRE.[Catering_Department] = FR.[Catering_Department] and 
            WRE.[Recipe_Web_Codes] = FR.[Recipe_Web_Codes] and 
            WRE.[Serving_Size] = FR.[Serving_Size] and 
            WRE.[Calories] = FR.[Calories] and 
            WRE.[Calories_From_Fat] = FR.[Calories_From_Fat] and 
            WRE.[Total_Fat] = FR.[Total_Fat] and 
            WRE.[Total_Fat_DV] = FR.[Total_Fat_DV] and 
            WRE.[Sat_Fat] = FR.[Sat_Fat] and 
            WRE.[Sat_Fat_DV] = FR.[Sat_Fat_DV] and 
            WRE.[Trans_Fat] = FR.[Trans_Fat] and 
            WRE.[Trans_Fat_DV] = FR.[Trans_Fat_DV] and 
            WRE.[Cholesterol] = FR.[Cholesterol] and 
            WRE.[Cholesterol_DV] = FR.[Cholesterol_DV] and 
            WRE.[Sodium] = FR.[Sodium] and 
            WRE.[Sodium_DV] = FR.[Sodium_DV] and
            WRE.[Total_Carb] = FR.[Total_Carb] and
            WRE.[Total_Carb_DV] = FR.[Total_Carb_DV] and
            WRE.[Dietary_Fiber] = FR.[Dietary_Fiber] and
            WRE.[Dietary_Fiber] = FR.[Dietary_Fiber] and
            WRE.[Sugars] = FR.[Sugars] and
            WRE.[Sugars_DV] = FR.[Sugars_DV] and
            WRE.[Protein] = FR.[Protein] and
            WRE.[Protein_DV] = FR.[Protein_DV]
            ) tbl 

            Left join [warehouse].[RecipesWarehouse] WRW on WRW.[Serve_Date] = tbl.[Serve_Date] 
            and WRW.[LocationId] = tbl.[LocationId] and WRW.[MealNameId] = tbl.[MealNameId] 
            and WRW.[MenuCategoryId] = tbl.[MenuCategoryId] 
            and tbl.RecipeId = WRW.RecipeId
            where RecipesWarehouseID is null");

                    myConnection.Open();
                    cmd.CommandText = qry;
                    var rowsAffected = cmd.ExecuteNonQuery();
                    LogWriter.LogWithMessage("SyncAllRecipesWarehouse Executed. Number of data inserted : " + rowsAffected);
                    return true;
                }
            }
            catch (Exception ex)
            {
                LogWriter.WriteErrorLog("EXCEPTION from SyncAllRecipesWarehouse: " + ex.Message);
                LogWriter.LogWithMessage("EXCEPTION from SyncAllRecipesWarehouse: " + ex.Message);
                return false;
            }
        }
        #endregion
    }
}
