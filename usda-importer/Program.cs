﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Linq;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.EnvironmentVariables;
using Microsoft.Extensions.Configuration.Json;

namespace UsdaSterling
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Initializing Sterling USDA import tool...");
            MainAsync().Wait();
            Console.WriteLine("Import is complete.");
            Console.WriteLine("Hit enter to view top 20 foods by protein content.");
            Console.ReadLine();

            var query = from n in UsdaDatabase.Current.Query<Nutrient, double, string>(UsdaDatabase.NutrientIndex)
                        join fi in UsdaDatabase.Current.Query<FoodItem, string>()
                            on (n.Key.Split('|')[0]).ToString() equals fi.Key
                        where n.Key.Split('|')[1].ToString() == "203" // protein
                        orderby n.Index descending
                        select Tuple.Create(fi, n.Index);
            foreach (var item in query.Take(20))
            {
                Console.WriteLine($"{item.Item1.LazyValue.Value.Description} has {item.Item2} protein per 100 grams.");
            }
            
            Console.WriteLine("Hit enter to view top 20 foods by carbohydate content.");
            Console.ReadLine();

            query = from n in UsdaDatabase.Current.Query<Nutrient, double, string>(UsdaDatabase.NutrientIndex)
                        join fi in UsdaDatabase.Current.Query<FoodItem, string>()
                            on n.Key.Split('|')[0] equals fi.Key
                        where n.Key.Split('|')[1] == "204" // carbohydate
                        orderby n.Index descending
                        select Tuple.Create(fi, n.Index);
            foreach (var item in query.Take(20))
            {
                Console.WriteLine($"{item.Item1.LazyValue.Value.Description} has {item.Item2} carbohydrate per 100 grams.");
            }

            Console.WriteLine("Hit enter to view top 20 foods by fat content.");
            Console.ReadLine();

            query = from n in UsdaDatabase.Current.Query<Nutrient, double, string>(UsdaDatabase.NutrientIndex)
                        join fi in UsdaDatabase.Current.Query<FoodItem, string>()
                            on n.Key.Split('|')[0] equals fi.Key
                        where n.Key.Split('|')[1] == "205" // fat
                        orderby n.Index descending
                        select Tuple.Create(fi, n.Index);
            foreach (var item in query.Take(20))
            {
                Console.WriteLine($"{item.Item1.LazyValue.Value.Description} has {item.Item2} fats per 100 grams.");
            }
        }

        public async static Task MainAsync()
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json")
                .AddEnvironmentVariables();

            var config = builder.Build();
            var transformer = new Transformer();
            var importer = new SterlingImporter();

            Console.WriteLine("Loading food groups...");

            FoodGroup[] groups = await transformer.Transform<FoodGroup>(config["USDA_FOOD_GROUP"], arr =>
            {
                return new FoodGroup
                {
                    Code = arr[0],
                    Description = arr[1]
                };
            });

            Console.WriteLine($"Parsed {groups.Length} food groups.");
            Console.WriteLine("Importing food groups...");
            importer.Import(groups);

            Console.WriteLine("Loading weights...");

            Weight[] weights = await transformer.Transform<Weight>(config["USDA_WEIGHT"], arr =>
            {
                return new Weight
                {
                    FoodId = arr[0],
                    Sequence = arr[1],
                    Amount = double.Parse(arr[2]),
                    Description = arr[3],
                    WeightGrams = double.Parse(arr[4])
                };
            });

            Console.WriteLine($"Parsed {weights.Length} weights.");
            Console.WriteLine("Loading nutrient definitions...");

            NutrientDefinition[] definitions = await transformer.Transform<NutrientDefinition>(config["USDA_NUTRIENT_DEFINITIONS"], arr =>
            {
                return new NutrientDefinition
                {
                    NutrientId = arr[0],
                    UnitOfMeasure = arr[1],
                    TagName = arr[2],
                    Description = arr[3],
                    SortOrder = int.Parse(arr[5])
                };
            });

            Console.WriteLine($"Parsed {definitions.Length} definitions.");
            Console.WriteLine("Loading nutrient data...");

            Nutrient[] nutrients = await transformer.Transform<Nutrient>(config["USDA_NUTRIENT_DATA"], arr =>
            {
                return new Nutrient
                {
                    FoodId = arr[0],
                    NutrientId = arr[1],
                    AmountInHundredGrams = double.Parse(arr[2])
                };
            });

            Console.WriteLine($"Parsed {nutrients.Length} nutrient data entries.");
            Console.WriteLine("Correlating nutrient data to definitions ...");

            foreach (var nutrient in nutrients)
            {
                var definition = definitions.FirstOrDefault(d => d.NutrientId == nutrient.NutrientId);
                nutrient.Definition = definition;
            }

            Console.WriteLine("Correlated.");

            Console.WriteLine("Importing nutrient data...");
            importer.Import(nutrients.Where(n => n.NutrientId == "203" || n.NutrientId == "204" || n.NutrientId == "205").ToArray()); // protein, carbs, fats
            
            Console.WriteLine("Loading food descriptions...");

            var foodLookup = new Dictionary<string, FoodItem>();
            FoodItem[] food = await transformer.Transform<FoodItem>(config["USDA_FOOD_ITEM"], arr =>
            {
                var item = new FoodItem
                {
                    FoodId = arr[0],
                    FoodGroupId = arr[1],
                    Description = arr[2],
                    ShortDescription = arr[3],
                    CommonNames = arr[4],
                    Inedible = arr[7]
                };
                foodLookup.Add(item.FoodId, item);
                return item;
            });

            Console.WriteLine($"Parsed {food.Length} food items.");

            Console.WriteLine("Correlating food groups...");
            foreach (var foodItem in food)
            {
                foodItem.Group = groups.FirstOrDefault(g => g.Code == foodItem.FoodGroupId);
            }
            Console.WriteLine("Correlated.");

            Console.WriteLine("Correlating weights...");

            var weightList = new List<Weight>();
            var foodId = string.Empty;
            foreach (var weight in weights.OrderBy(w => w.FoodId))
            {
                if (weight.FoodId != foodId)
                {
                    if (weightList.Count > 0)
                    {
                        foodLookup[foodId].Weights = weightList.ToArray();
                    }
                    weightList.Clear();
                    foodId = weight.FoodId;
                }
                weightList.Add(weight);
            }
            if (weightList.Count > 0)
            {
                foodLookup[foodId].Weights = weightList.ToArray();
            }
            Console.WriteLine("Correlated.");

            Console.WriteLine("Sorting nutrients...");
            var sortedList = nutrients.OrderBy(n => n.FoodId).ThenBy(n => n.Definition.SortOrder).ToArray();
            var nutrientList = new List<Nutrient>();
            foodId = string.Empty;
            Console.WriteLine("Correlating nutrients...");
            foreach (var nutrient in sortedList)
            {
                if (nutrient.FoodId != foodId)
                {
                    if (nutrientList.Count > 0)
                    {
                        foodLookup[foodId].Nutrients = nutrientList.ToArray();
                    }
                    nutrientList.Clear();
                    foodId = nutrient.FoodId;
                }
                nutrientList.Add(nutrient);
            }
            if (nutrientList.Count > 0)
            {
                foodLookup[foodId].Nutrients = nutrientList.ToArray();
            }
            Console.WriteLine("Correlated.");

            Console.WriteLine("Importing food items to Sterling (this may take several minutes)...");
            importer.Import(food.Where(f => f.FoodGroupId == "2000").ToArray()); // cereal/grains
        }
    }
}
