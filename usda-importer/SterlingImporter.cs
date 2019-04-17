using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;

namespace UsdaSterling
{
    public class SterlingImporter
    {
        public void Import<T>(T[] items) where T: class, new()
        {
            Console.Write($"Importing {items.Length} items. This may take several minutes.");
            //var done = false;
            var bg = UsdaDatabase.Current.SaveAsync((IList<T>) items);
            //var pct = 0;
            /*
            bg.WorkerReportsProgress = true;
            bg.ProgressChanged += (o, p) => {
                if (pct != p.ProgressPercentage) {
                    Console.Write($"...{p.ProgressPercentage}%");
                    pct = p.ProgressPercentage;
                }
            };
            bg.RunWorkerCompleted += (a, b) => done = true;
            bg.RunWorkerAsync();
            */
            var sleep = 100;
            while (!bg.IsCompleted)
            {
                Thread.Sleep(sleep);
                if (sleep < 1000) {
                    sleep += 100;
                }
                Console.Write(".");
            }
            Console.WriteLine();
#if DEBUG
            Console.WriteLine(" Faulted!: " + bg.IsFaulted);
            if (bg.Exception != null)
            {
                Console.WriteLine(bg.Exception.ToString());
                // Get stack trace for the exception with source file information
                var st = new System.Diagnostics.StackTrace(bg.Exception, true);
                //Get the first stack frame
                System.Diagnostics.StackFrame frame = st.GetFrame(0);

                //Get the file name
                string fileName = frame.GetFileName();

                //Get the method name
                string methodName = frame.GetMethod().Name;

                //Get the line number from the stack frame
                int line = frame.GetFileLineNumber();

                //Get the column number
                int col = frame.GetFileColumnNumber();
                Console.WriteLine(fileName);
                Console.WriteLine(methodName);
                Console.WriteLine(line);
                Console.WriteLine(col);
            }
#endif

            bg.Dispose();
            Console.WriteLine($"Successfully imported {Count<T>()} documents. Your collection is ready!");
        }

        private int Count<T>()
        {
            IEnumerable<string> query;
            if (typeof(T) == typeof(FoodGroup))
            {
                query = from fg in UsdaDatabase.Current.Query<FoodGroup, string>()
                    select fg.Key;
            }
            else if (typeof(T) == typeof(Nutrient))
            {
                query = from n in UsdaDatabase.Current.Query<Nutrient, string>()
                    select n.Key;
            }
            else 
            {
                query = from fi in UsdaDatabase.Current.Query<FoodItem, string>()
                    select fi.Key;
            }
            return query.Count();
        } 
    }
}