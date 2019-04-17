using Sterling.Core.Events;
using Sterling.Core.Exceptions;
using Sterling.Core.Indexes;
using Sterling.Core.Keys;
using Sterling.Core.Serialization;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Sterling.Core.Database
{
    /// <summary>
    ///     Base class for a sterling database instance
    /// </summary>
    public abstract class BaseDatabaseInstance : ISterlingDatabaseInstance
    {
        /// <summary>
        ///     Master database driver
        /// </summary>
        public ISterlingDriver Driver { get; private set; }

        /// <summary>
        ///     Master database locks
        /// </summary>
        private static readonly ConcurrentDictionary<Type, object> _locks = new ConcurrentDictionary<Type, object>();

        /// <summary>
        ///     Workers to track/flush
        /// </summary>
        //private readonly List<Task> _workers = new List<Task>();
        private readonly ConcurrentDictionary<Task, CancellationTokenSource> _workers = new ConcurrentDictionary<Task, CancellationTokenSource>();



        /// <summary>
        ///     List of triggers
        /// </summary>
        private readonly ConcurrentDictionary<Type, List<ISterlingTrigger>> _triggers =
            new ConcurrentDictionary<Type, List<ISterlingTrigger>>();

        /// <summary>
        ///     The table definitions
        /// </summary>
        //internal readonly Dictionary<Type, ITableDefinition> TableDefinitions = new Dictionary<Type, ITableDefinition>();
        internal readonly ConcurrentDictionary<Type, ITableDefinition> TableDefinitions = new ConcurrentDictionary<Type, ITableDefinition>();

        /// <summary>
        ///     Serializer
        /// </summary>
        internal ISterlingSerializer Serializer { get; set; }

        /// <summary>
        ///     Called when this should be deactivated
        /// </summary>
        internal static void Deactivate()
        {
            _locks.Clear();
        }

        /// <summary>
        ///     The base database instance
        /// </summary>
        protected BaseDatabaseInstance()
        {
            if (!_locks.TryAdd(GetType(), new object()))
            {
                throw new SterlingDuplicateDatabaseException(this);
            }
        }

        public void Unload()
        {
            Flush();
        }

        /// <summary>
        ///     Must return an object for synchronization
        /// </summary>
        public object Lock
        {
            get { return _locks[GetType()]; }
        }

        /// <summary>
        ///     Register a trigger
        /// </summary>
        /// <param name="trigger">The trigger</param>
        public void RegisterTrigger<T, TKey>(BaseSterlingTrigger<T, TKey> trigger) where T : class, new()
        {
            var triggerList = this._triggers.GetOrAdd(typeof(T), x => new List<ISterlingTrigger>());

            lock (((ICollection)triggerList).SyncRoot)
            {
                triggerList.Add(trigger);
            }
        }

        /// <summary>
        ///     Unregister the trigger
        /// </summary>
        /// <param name="trigger">The trigger</param>
        public void UnregisterTrigger<T, TKey>(BaseSterlingTrigger<T, TKey> trigger) where T : class, new()
        {
            if (!this._triggers.ContainsKey(typeof(T))) return;

            if (this._triggers.TryGetValue(typeof(T), out List<ISterlingTrigger> triggerlist))
            {
                lock (((ICollection)triggerlist).SyncRoot)
                {
                    if (triggerlist.Contains(trigger))
                    {
                        triggerlist.Remove(trigger);
                    }
                }
            }
        }

        /// <summary>
        ///     Fire the triggers for a type
        /// </summary>
        /// <param name="type">The target type</param>
        private IEnumerable<ISterlingTrigger> _TriggerList(Type type)
        {
            if (this._triggers.TryGetValue(type, out List<ISterlingTrigger> triggerlist))
            {
                List<ISterlingTrigger> triggers;

                lock (((ICollection)triggerlist).SyncRoot)
                {
                    triggers = new List<ISterlingTrigger>(triggerlist);
                }
                return triggers;
            }

            return Enumerable.Empty<ISterlingTrigger>();
        }

        /// <summary>
        ///     The name of the database instance
        /// </summary>
        public virtual string Name { get { return GetType().FullName; } }


        /// <summary>
        ///     The type dictating which objects should be ignored
        /// </summary>
        public virtual Type IgnoreAttribute { get { return typeof(SterlingIgnoreAttribute); } }

        /// <summary>
        ///     Method called from the constructor to register tables
        /// </summary>
        /// <returns>The list of tables for the database</returns>
        protected abstract List<ITableDefinition> RegisterTables();

        /// <summary>
        ///     Register any type resolvers.
        /// </summary>
        protected internal virtual void RegisterTypeResolvers()
        {
        }

        /// <summary>
        ///     Registers any property converters.
        /// </summary>
        protected internal virtual void RegisterPropertyConverters()
        {
        }

        /// <summary>
        /// Register a class responsible for type resolution.
        /// </summary>
        /// <param name="typeInterceptor"></param>
        protected void RegisterTypeResolver(ISterlingTypeResolver typeInterceptor)
        {
            TableTypeResolver.RegisterTypeResolver(typeInterceptor);
        }

        private readonly Dictionary<Type, ISterlingPropertyConverter> _propertyConverters = new Dictionary<Type, ISterlingPropertyConverter>();

        /// <summary>
        ///     Registers a property converter.
        /// </summary>
        /// <param name="propertyConverter">The property converter</param>
        protected void RegisterPropertyConverter(ISterlingPropertyConverter propertyConverter)
        {
            this._propertyConverters.Add(propertyConverter.IsConverterFor(), propertyConverter);
        }

        /// <summary>
        ///     Gets the property converter for the given type, or returns null if none is found.
        /// </summary>
        /// <param name="type">The type</param>
        /// <param name="propertyConverter">The property converter</param>
        /// <returns>True if there is a registered property converter.</returns>
        public bool TryGetPropertyConverter(Type type, out ISterlingPropertyConverter propertyConverter)
        {
            return this._propertyConverters.TryGetValue(type, out propertyConverter);
        }

        /// <summary>
        ///     Returns a table definition 
        /// </summary>
        /// <typeparam name="T">The type of the table</typeparam>
        /// <typeparam name="TKey">The type of the key</typeparam>
        /// <param name="keyFunction">The key mapping function</param>
        /// <returns>The table definition</returns>
        public ITableDefinition CreateTableDefinition<T, TKey>(Func<T, TKey> keyFunction) where T : class, new()
        {
            return new TableDefinition<T, TKey>(this.Driver,
                                                Load<T, TKey>, keyFunction);
        }

        /// <summary>
        ///     Get the list of table definitions
        /// </summary>
        /// <returns>The list of table definitions</returns>
        public IEnumerable<ITableDefinition> GetTableDefinitions()
        {
            return new List<ITableDefinition>(this.TableDefinitions.Values);
        }

        /// <summary>
        ///     Get the list of table definitions
        /// </summary>
        /// <returns>The list of table definitions</returns>
        public ITableDefinition GetTableDefinition(object instance)
        {
            if (!this.TableDefinitions.TryGetValue(instance.GetType(), out ITableDefinition tableDefinition))
            {
                return tableDefinition;
            }
            return null;
        }

        /// <summary>
        ///     Register a new table definition
        /// </summary>
        /// <param name="tableDefinition">The new table definition</param>
        public void RegisterTableDefinition(ITableDefinition tableDefinition)
        {
            this.TableDefinitions.TryAdd(tableDefinition.TableType, tableDefinition);
        }

        /// <summary>
        ///     Call to publish tables 
        /// </summary>
        internal void PublishTables(ISterlingDriver driver)
        {
            this.Driver = driver;

            foreach (var table in RegisterTables())
            {
                if (!this.TableDefinitions.TryAdd(table.TableType, table))
                {
                    throw new SterlingDuplicateTypeException(table.TableType, this.Name);
                }
            }

            this.Driver.PublishTables(this.TableDefinitions);
        }

        /// <summary>
        ///     True if it is registered with the sterling engine
        /// </summary>
        /// <param name="instance">The instance</param>
        /// <returns>True if it can be persisted</returns>
        public bool IsRegistered<T>(T instance) where T : class
        {
            return IsRegistered(typeof(T));
        }

        /// <summary>
        ///     Non-generic registration check
        /// </summary>
        /// <param name="type">The type</param>
        /// <returns>True if it is registered</returns>
        public bool IsRegistered(Type type)
        {
            return this.TableDefinitions.ContainsKey(type);
        }

        /// <summary>
        ///     Get the key for an object
        /// </summary>
        /// <param name="instance">The instance</param>
        /// <returns>The key</returns>
        public object GetKey(object instance)
        {
            if (!this.TableDefinitions.TryGetValue(instance.GetType(), out ITableDefinition tableDefinition))
            {
                throw new SterlingTableNotFoundException(instance.GetType(), this.Name);
            }

            return tableDefinition.FetchKeyFromInstance(instance);
        }

        /// <summary>
        ///     Get the key for an object
        /// </summary>
        /// <param name="table">The instance type</param>
        /// <returns>The key type</returns>
        public Type GetKeyType(Type table)
        {
            //if (!IsRegistered(table))
            if (!this.TableDefinitions.TryGetValue(table, out ITableDefinition tableDefinition))
            {
                throw new SterlingTableNotFoundException(table, this.Name);
            }

            return tableDefinition.KeyType;
        }

        /// <summary>
        ///     Query (keys only)
        /// </summary>
        /// <typeparam name="T">The type to query</typeparam>
        /// <typeparam name="TKey">The type of the key</typeparam>
        /// <returns>The list of keys to query</returns>
        public List<TableKey<T, TKey>> Query<T, TKey>() where T : class, new()
        {
            //if (!IsRegistered(typeof(T)))
            if (!this.TableDefinitions.TryGetValue(typeof(T), out ITableDefinition tableDefinition))
            {
                throw new SterlingTableNotFoundException(typeof(T), this.Name);
            }

            return
                new List<TableKey<T, TKey>>(
                    ((TableDefinition<T, TKey>)tableDefinition).KeyList.Query);
        }

        /// <summary>
        ///     Query an index
        /// </summary>
        /// <typeparam name="T">The table type</typeparam>
        /// <typeparam name="TIndex">The index type</typeparam>
        /// <typeparam name="TKey">The key type</typeparam>
        /// <param name="indexName">The name of the index</param>
        /// <returns>The indexed items</returns>
        public List<TableIndex<T, TIndex, TKey>> Query<T, TIndex, TKey>(string indexName) where T : class, new()
        {
            if (string.IsNullOrEmpty(indexName))
            {
                throw new ArgumentNullException("indexName");
            }

            if (!this.TableDefinitions.TryGetValue(typeof(T), out ITableDefinition tableDefinition))
            {
                throw new SterlingTableNotFoundException(typeof(T), this.Name);
            }

            var tableDef = (TableDefinition<T, TKey>)tableDefinition;

            //if (!tableDef.Indexes.ContainsKey(indexName))
            if (!tableDef.Indexes.TryGetValue(indexName, out IIndexCollection indexCollection))
            {
                throw new SterlingIndexNotFoundException(indexName, typeof(T));
            }

            if (!(indexCollection is IndexCollection<T, TIndex, TKey> collection))
            {
                throw new SterlingIndexNotFoundException(indexName, typeof(T));
            }

            return new List<TableIndex<T, TIndex, TKey>>(collection.Query);
        }

        /// <summary>
        ///     Query an index
        /// </summary>
        /// <typeparam name="T">The table type</typeparam>
        /// <typeparam name="TIndex1">The first index type</typeparam>
        /// <typeparam name="TIndex2">The second index type</typeparam>
        /// <typeparam name="TKey">The key type</typeparam>
        /// <param name="indexName">The name of the index</param>
        /// <returns>The indexed items</returns>
        public List<TableIndex<T, Tuple<TIndex1, TIndex2>, TKey>> Query<T, TIndex1, TIndex2, TKey>(string indexName)
            where T : class, new()
        {
            if (string.IsNullOrEmpty(indexName))
            {
                throw new ArgumentNullException("indexName");
            }

            if (!this.TableDefinitions.TryGetValue(typeof(T), out ITableDefinition tableDefinition))
            {
                throw new SterlingTableNotFoundException(typeof(T), this.Name);
            }

            var tableDef = (TableDefinition<T, TKey>)tableDefinition;

            if (!tableDef.Indexes.TryGetValue(indexName, out IIndexCollection indexCollection))
            {
                throw new SterlingIndexNotFoundException(indexName, typeof(T));
            }

            if (!(indexCollection is IndexCollection<T, TIndex1, TIndex2, TKey> collection))
            {
                throw new SterlingIndexNotFoundException(indexName, typeof(T));
            }

            return new List<TableIndex<T, Tuple<TIndex1, TIndex2>, TKey>>(collection.Query);
        }

        /// <summary>
        ///     Save an instance against a base class table definition
        /// </summary>
        /// <typeparam name="T">The table type</typeparam>
        /// <typeparam name="TKey">Save it</typeparam>
        /// <param name="instance">An instance or sub-class of the table type</param>
        /// <returns></returns>
        public bool SaveAs<T, TKey>(T instance, out TKey key) where T : class, new()
        {
            var tmpval = SaveAs<T>(instance, out object tmpkey);
            key = (TKey)tmpkey;
            return tmpval;
        }

        /// <summary>
        ///     Save an instance against a base class table definition
        /// </summary>
        /// <typeparam name="T">The table type</typeparam>
        /// <param name="instance">An instance or sub-class of the table type</param>
        /// <returns></returns>
        public bool SaveAs<T>(T instance, out object key) where T : class, new()
        {
            var tableType = typeof(T);
            return Save(instance.GetType(), tableType, instance, new CycleCache(), out key);
        }

        /// <summary>
        ///     Save against a base class when key is not known
        /// </summary>
        /// <param name="tableType"></param>
        /// <param name="instance">The instance</param>
        /// <returns>The key</returns>
        public bool SaveAs(Type tableType, object instance, out object key)
        {
            if (!instance.GetType().IsSubclassOf(tableType) || instance.GetType() != tableType)
            {
                throw new SterlingException(string.Format("{0} is not of type {1}", instance.GetType().Name, tableType.Name));
            }

            return Save(tableType, instance, out key);
        }

        /// <summary>
        ///     Save it
        /// </summary>
        /// <typeparam name="T">The instance type</typeparam>
        /// <typeparam name="TKey">Save it</typeparam>
        /// <param name="instance">The instance</param>
        public bool Save<T, TKey>(T instance, out TKey key) where T : class, new()
        {
            var tmpval = Save(typeof(T), instance, out object tmpkey);
            key = (TKey)tmpkey;
            return tmpval;
        }

        /// <summary>
        ///     Entry point for save
        /// </summary>
        /// <param name="type">Type to save</param>
        /// <param name="instance">Instance</param>
        /// <returns>The key saved</returns>
        public bool Save(Type type, object instance, out object key)
        {
            return Save(type, type, instance, new CycleCache(), out key);
        }

        /// <summary>
        ///     Save when key is not known
        /// </summary>
        /// <param name="actualType">The type of instance to save</param>
        /// <param name="tableType">The table type to save to</param>
        /// <param name="instance">The instance</param>
        /// <param name="cache">Cycle cache</param>
        /// <returns>The key</returns>
        public bool Save(Type actualType, Type tableType, object instance, CycleCache cache, out object key)
        {
            bool retVal;
            if (retVal = SaveInternal(actualType, tableType, instance, cache, out key))
                _RaiseOperation(SterlingOperation.Save, tableType, key, instance);
            return retVal;
        }

        bool SaveInternal(Type actualType, Type tableType, object instance, CycleCache cache, out object key)
        {
            if (!this.TableDefinitions.TryGetValue(tableType, out ITableDefinition tableDefinition))
            {
                throw new SterlingTableNotFoundException(instance.GetType(), this.Name);
            }

            if (!tableDefinition.IsDirty(instance))
            {
                key = tableDefinition.FetchKeyFromInstance(instance);
                return false;
            }

            // call any before save triggers 
            foreach (var trigger in _TriggerList(tableType).Where(trigger => !trigger.BeforeSave(actualType, instance)))
            {
                throw new SterlingTriggerException(
                    Exceptions.Exceptions.BaseDatabaseInstance_Save_Save_suppressed_by_trigger, trigger.GetType());
            }

            key = tableDefinition.FetchKeyFromInstance(instance);

            int keyIndex;

            if (cache.Check(instance))
            {
                return false;
            }

            lock (tableDefinition)
            {
                if (cache.Check(instance))
                {
                    return false;
                }

                cache.Add(tableType, instance, key);
                keyIndex = tableDefinition.Keys.AddKey(key);
            }

            this.Driver.Save(actualType, tableType, instance, keyIndex, cache);

            // update the indexes
            foreach (var index in tableDefinition.Indexes.Values)
            {
                index.AddIndex(instance, key);
            }

            // call post-save triggers
            foreach (var trigger in _TriggerList(tableType))
            {
                trigger.AfterSave(actualType, instance);
            }

            return true;
        }

        /// <summary>
        ///     Save when key is not known
        /// </summary>
        /// <typeparam name="T">The type of the instance</typeparam>
        /// <param name="instance">The instance</param>
        /// <returns>The key</returns>
        public bool Save<T>(T instance) where T : class, new()
        {
            return Save(typeof(T), instance, out object throwaway);
        }

        /// <summary>
        ///     Save when key is not known
        /// </summary>
        /// <typeparam name="T">The type of the instance</typeparam>
        /// <param name="instance">The instance</param>
        /// <returns>The key</returns>
        public bool Save<T>(T instance, out object key) where T : class, new()
        {
            return Save(typeof(T), instance, out key);
        }

        /// <summary>
        ///     Save asynchronously
        /// </summary>
        /// <typeparam name="T">The type to save</typeparam>
        /// <param name="list">A list of items to save</param>
        /// <returns>A unique identifier for the batch</returns>
        public Task<IList<object>> SaveAsync<T>(IList<T> list)
        {
            return SaveAsync((IList)list);
        }

        /// <summary>
        ///     Save asynchronously
        /// </summary>
        /// <typeparam name="T">The type to save</typeparam>
        /// <param name="list">A list of items to save</param>
        /// <param name="cancellationTokenSource">CancellationTokenSource when activated cancels the save</param>
        /// <returns>A unique identifier for the batch</returns>
        public Task<IList<object>> SaveAsync<T>(IList<T> list, CancellationTokenSource cancellationTokenSource)
        {
            return SaveAsync((IList)list, cancellationTokenSource);
        }

        /// <summary>
        ///     Non-generic asynchronous save
        /// </summary>
        /// <param name="list">The list of items</param>
        /// <returns>A unique job identifier</returns>
        public Task<IList<object>> SaveAsync(IList list)
        {
            return SaveAsync(list, default(CancellationTokenSource));
        }

        /// <summary>
        ///     Non-generic asynchronous save
        /// </summary>
        /// <param name="list">The list of items</param>
        /// <param name="cancellationTokenSource">CancellationTokenSource when activated cancels the save</param>
        /// <returns>list containing all of the added items</returns>
        public async Task<IList<object>> SaveAsync(IList list, CancellationTokenSource cancellationTokenSource)
        {
            var added = new List<object>();
            if (list == null)
            {
                throw new ArgumentNullException("list");
            }

            if (list.Count == 0)
            {
                return added;
            }

            if (!IsRegistered(list[0].GetType()))
            {

                throw new SterlingTableNotFoundException(list[0].GetType(), this.Name);
            }

            var ctsToken = cancellationTokenSource?.Token ?? CancellationToken.None;

            Task myTask;

            this._workers.TryAdd(myTask =
                Task.Run(() =>
                {
                    foreach (var item in list)
                    {
                        if (ctsToken.IsCancellationRequested)
                            return;

                        if (SaveInternal(item.GetType(), item.GetType(), item, new CycleCache(), out object key))
                        //Save(item.GetType(), item, out object key))
                        {
                            added.Add(key);
                            _RaiseOperation(SterlingOperation.Save, item.GetType(), key);
                        }
                    }
                }, ctsToken
            ), cancellationTokenSource);

            try
            {
                await myTask;
            }
            finally
            {
                this._workers.TryRemove(myTask, out CancellationTokenSource cts);
            }

            return added;
        }

        /// <summary>
        ///     Flush all keys and indexes to storage
        /// </summary>
        public void Flush()
        {
            if (_locks == null || !_locks.ContainsKey(GetType())) return;

            lock (this.Lock)
            {
                foreach (var def in this.TableDefinitions.Values)
                {
                    def.Keys.Flush();

                    foreach (var idx in def.Indexes.Values)
                    {
                        idx.Flush();
                    }
                }
            }

            _RaiseOperation(SterlingOperation.Flush, GetType(), this.Name);
        }

        /// <summary>
        ///     Load it 
        /// </summary>
        /// <typeparam name="T">The type to load</typeparam>
        /// <typeparam name="TKey">The key type</typeparam>
        /// <param name="key">The value of the key</param>
        /// <returns>The instance</returns>
        public T Load<T, TKey>(TKey key) where T : class, new()
        {
            return (T)Load(typeof(T), key);
        }

        /// <summary>
        ///     Load it (key type not typed)
        /// </summary>
        /// <typeparam name="T">The type to load</typeparam>
        /// <param name="key">The key</param>
        /// <returns>The instance</returns>
        public T Load<T>(object key) where T : class, new()
        {
            return (T)Load(typeof(T), key);
        }

        /// <summary>
        ///     Load entry point with new cycle cache
        /// </summary>
        /// <param name="type">The type to load</param>
        /// <param name="key">The key</param>
        /// <returns>The object</returns>
        public object Load(Type type, object key)
        {
            return Load(type, key, new CycleCache());
        }

        /// <summary>
        ///     Load it without knowledge of the key type
        /// </summary>
        /// <param name="type">The type to load</param>
        /// <param name="key">The key</param>
        /// <param name="cache">Cache queue</param>
        /// <returns>The instance</returns>
        public object Load(Type type, object key, CycleCache cache)
        {
            var newType = type;
            var assignable = false;
            var keyIndex = -1;

            if (!this.TableDefinitions.ContainsKey(type))
            {
                // check if type is a base type
                foreach (var t in this.TableDefinitions.Keys.Where(type.IsAssignableFrom))
                {
                    assignable = true;

                    var tableDefinition = this.TableDefinitions[t];

                    lock (tableDefinition)
                    {
                        keyIndex = tableDefinition.Keys.GetIndexForKey(key);
                    }

                    if (keyIndex < 0) continue;

                    newType = t;
                    break;
                }
            }
            else
            {
                var tableDefinition = this.TableDefinitions[newType];
                lock (tableDefinition)
                {
                    keyIndex = tableDefinition.Keys.GetIndexForKey(key);
                }
            }

            if (!assignable)
            {
                if (!this.TableDefinitions.ContainsKey(type))
                {
                    throw new SterlingTableNotFoundException(type, this.Name);
                }
            }

            if (keyIndex < 0)
            {
                return null;
            }

            var obj = GetInstance(newType, key, keyIndex, cache);

            _RaiseOperation(SterlingOperation.Load, newType, key);
            return obj;
        }

        /// <summary>
        ///     Gets an instance by key
        /// </summary>
        private object GetInstance(Type type, object key, int keyIndex)
        {
            return GetInstance(type, key, keyIndex, new CycleCache());
        }

        /// <summary>
        ///     Gets an instance by key
        /// </summary>
        private object GetInstance(Type type, object key, int keyIndex, CycleCache cache)
        {
            var obj = cache.CheckKey(type, key);

            if (obj != null)
            {
                return obj;
            }

            if (keyIndex < 0)
            {
                return null;
            }

            obj = this.Driver.Load(type, key, keyIndex, cache);

            return obj;
        }

        /// <summary>
        ///     Delete it 
        /// </summary>
        /// <typeparam name="T">The type to delete</typeparam>
        /// <param name="instance">The instance</param>
        public void Delete<T>(T instance) where T : class
        {
            Delete(typeof(T), this.TableDefinitions[typeof(T)].FetchKeyFromInstance(instance));
        }

        /// <summary>
        ///     Delete it (non-generic)
        /// </summary>
        /// <param name="type">The type</param>
        /// <param name="key">The key</param>
        public void Delete(Type type, object key)
        {
            //if (!this.TableDefinitions.ContainsKey(type))
            if (!this.TableDefinitions.TryGetValue(type, out ITableDefinition tableDefinition))
            {
                throw new SterlingTableNotFoundException(type, this.Name);
            }

            // call any before save triggers 
            foreach (var trigger in _TriggerList(type).Where(trigger => !trigger.BeforeDelete(type, key)))
            {
                throw new SterlingTriggerException(
                    string.Format(Exceptions.Exceptions.BaseDatabaseInstance_Delete_Delete_failed_for_type, type),
                    trigger.GetType());
            }

            var keyEntry = tableDefinition.Keys.GetIndexForKey(key);

            //var deleted = GetInstance(type, key, keyEntry);
            this.Driver.Delete(type, keyEntry);

            tableDefinition.Keys.RemoveKey(key);
            foreach (var index in tableDefinition.Indexes.Values)
            {
                index.RemoveIndex(key);
            }

            //_RaiseOperation(SterlingOperation.Delete, type, key, deleted);
            _RaiseOperation(SterlingOperation.Delete, type, key);
        }

        /// <summary>
        ///     Truncate all records for a type
        /// </summary>
        /// <param name="type">The type</param>
        public void Truncate(Type type)
        {
            if (this._workers.Count > 0)
            {
                throw new SterlingException(
                    Exceptions.Exceptions.BaseDatabaseInstance_Truncate_Cannot_truncate_when_background_operations);
            }

            if (_locks == null || !_locks.ContainsKey(GetType())) return;

            lock (this.Lock)
            {
                this.Driver.Truncate(type);

                var tableDefinition = this.TableDefinitions[type];
                tableDefinition.Keys.Truncate();
                foreach (var index in tableDefinition.Indexes.Values)
                {
                    index.Truncate();
                }
            }

            _RaiseOperation(SterlingOperation.Truncate, type, null);
        }

        /// <summary>
        ///     Purge the entire database - wipe it clean!
        /// </summary>
        public void Purge()
        {
            Purge(default(CancellationToken), Timeout.Infinite);
        }

        /// <summary>
        ///     Purge the entire database - wipe it clean!
        /// </summary>
        /// <param name="millisecondsTimeout">Timeout to wait for running tasks</param>
        public void Purge(int millisecondsTimeout)
        {
            Purge(default(CancellationToken), millisecondsTimeout);
        }

        /// <summary>
        ///     Purge the entire database - wipe it clean!
        /// </summary>
        /// <param name="cancellationToken">Cancellation token to halt cancelling running tasks</param>
        public void Purge(CancellationToken cancellationToken)
        {
            Purge(cancellationToken, Timeout.Infinite);
        }

        /// <summary>
        ///     Purge the entire database - wipe it clean!
        /// </summary>
        /// <param name="cancellationToken">Cancellation token to halt cancelling running tasks</param>
        /// <param name="millisecondsTimeout">Timeout to wait for running tasks</param>
        public void Purge(CancellationToken cancellationToken, int millisecondsTimeout)
        {
            if (_locks == null || !_locks.ContainsKey(GetType())) return;

            lock (this.Lock)
            {
                //var delay = 0;

                // cancel all async operations, accumulate delays

                foreach (var worker in this._workers)
                {
                    if (!worker.Key.IsCompleted)
                        worker.Value.Cancel();
                    //delay += 100;
                }

                Task.WaitAll(this._workers.Select(x => x.Key).ToArray(), millisecondsTimeout, cancellationToken);
                this._workers.Clear();

                this.Driver.Purge();

                // clear key lists from memory
                foreach (var table in this.TableDefinitions.Keys)
                {
                    var tableDefinition = this.TableDefinitions[table];
                    tableDefinition.Keys.Truncate();
                    foreach (var index in tableDefinition.Indexes.Values)
                    {
                        index.Truncate();
                    }
                }
            }

            _RaiseOperation(SterlingOperation.Purge, GetType(), this.Name);
        }

        /// <summary>
        ///     Refresh indexes and keys from disk
        /// </summary>
        public void Refresh()
        {
            foreach (var table in this.TableDefinitions)
            {
                table.Value.Refresh();
            }
        }

        /// <summary>
        ///     Raise an operation
        /// </summary>
        /// <remarks>
        ///     Only send if access to the UI thread is available
        /// </remarks>
        /// <param name="operation">The operation</param>
        /// <param name="targetType">Target type</param>
        /// <param name="key">Key</param>
        private void _RaiseOperation(SterlingOperation operation, Type targetType, object key)
        {
            _RaiseOperation(operation, targetType, key, null);
        }

        /// <summary>
        ///     Raise an operation
        /// </summary>
        /// <remarks>
        ///     Only send if access to the UI thread is available
        /// </remarks>
        /// <param name="operation">The operation</param>
        /// <param name="targetType">Target type</param>
        /// <param name="instance">instance affected</param>
        /// <param name="key">Key</param>
        private void _RaiseOperation(SterlingOperation operation, Type targetType, object key, object instance)
        {
            var handler = SterlingOperationPerformed;

            if (handler == null) return;

            handler(this, new SterlingOperationArgs(operation, targetType, key));
        }

        /// <summary>
        ///     Called when an operation is performed on a table
        /// </summary>
        public event EventHandler<SterlingOperationArgs> SterlingOperationPerformed;
    }
}