using Sterling.Core.Database;
using Sterling.Core.Events;
using Sterling.Core.Indexes;
using Sterling.Core.Keys;
using Sterling.Core.Serialization;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Threading;
using System.Threading.Tasks;

namespace Sterling.Core
{
    /// <summary>
    ///     The sterling database instance
    /// </summary>
    public interface ISterlingDatabaseInstance : ISterlingLock
    {
        /// <summary>
        ///     The driver
        /// </summary>
        ISterlingDriver Driver { get; }

        /// <summary>
        ///     Register a trigger
        /// </summary>
        /// <param name="trigger">The trigger</param>
        void RegisterTrigger<T, TKey>(BaseSterlingTrigger<T, TKey> trigger) where T : class, new();

        /// <summary>
        ///     Unregister the trigger
        /// </summary>
        /// <param name="trigger">The trigger</param>
        void UnregisterTrigger<T, TKey>(BaseSterlingTrigger<T, TKey> trigger) where T : class, new();

        /// <summary>
        ///     The name of the database instance
        /// </summary>
        string Name { get; }

        /// <summary>
        ///     The type dictating which objects should be ignored
        /// </summary>
        Type IgnoreAttribute { get; }

        /// <summary>
        ///     True if it is registered with the sterling engine
        /// </summary>
        /// <param name="instance">The instance</param>
        /// <returns>True if it can be persisted</returns>
        bool IsRegistered<T>(T instance) where T : class;

        /// <summary>
        ///     Non-generic registration check
        /// </summary>
        /// <param name="type">The type</param>
        /// <returns>True if it is registered</returns>
        bool IsRegistered(Type type);

        /// <summary>
        ///     Get the key for an object
        /// </summary>
        /// <param name="instance">The instance</param>
        /// <returns>The key</returns>
        object GetKey(object instance);

        /// <summary>
        ///     Get the key for an object
        /// </summary>
        /// <param name="table">The instance type</param>
        /// <returns>The key type</returns>
        Type GetKeyType(Type table);




        /// <summary>
        ///     Query (keys only)
        /// </summary>
        /// <typeparam name="T">The type to query</typeparam>
        /// <typeparam name="TKey">The type of the key</typeparam>
        /// <returns>The list of keys to query</returns>
        List<TableKey<T, TKey>> Query<T, TKey>() where T : class, new();

        /// <summary>
        ///     Query (index)
        /// </summary>
        /// <typeparam name="T">The type to query</typeparam>
        /// <typeparam name="TIndex">The type of the index</typeparam>
        /// <typeparam name="TKey">The type of the key</typeparam>
        /// <param name="indexName">The name of the index</param>
        /// <returns>The list of indexes to query</returns>
        List<TableIndex<T, TIndex, TKey>> Query<T, TIndex, TKey>(string indexName) where T : class, new();

        /// <summary>
        ///     Query (index)
        /// </summary>
        /// <typeparam name="T">The type to query</typeparam>
        /// <typeparam name="TIndex1">The type of the index</typeparam>
        /// <typeparam name="TIndex2">The type of the index</typeparam>
        /// <typeparam name="TKey">The type of the key</typeparam>
        /// <param name="indexName">The name of the index</param>
        /// <returns>The list of indexes to query</returns>    
        List<TableIndex<T, Tuple<TIndex1, TIndex2>, TKey>> Query<T, TIndex1, TIndex2, TKey>(string indexName)
            where T : class, new();

        /// <summary>
        ///     Save it
        /// </summary>
        /// <typeparam name="T">The instance type</typeparam>
        /// <typeparam name="TKey">Save it</typeparam>
        /// <param name="instance">The instance</param>
        bool Save<T, TKey>(T instance, out TKey key) where T : class, new();

        /// <summary>
        ///     Save a sub-class under a base class table definition
        /// </summary>
        /// <typeparam name="T">The table type</typeparam>
        /// <typeparam name="TKey">Save it</typeparam>
        /// <param name="instance">An instance or sub-class of the table type</param>
        /// <returns></returns>
        bool SaveAs<T, TKey>(T instance, out TKey key) where T : class, new();

        /// <summary>
        ///     Save it (no knowledge of key)
        /// </summary>
        /// <typeparam name="T">The type</typeparam>
        /// <param name="instance">The instance</param>
        /// <returns>The key</returns>
        bool Save<T>(T instance) where T : class, new();

        /// <summary>
        ///     Save it (no knowledge of key)
        /// </summary>
        /// <typeparam name="T">The type</typeparam>
        /// <param name="instance">The instance</param>
        /// <returns>The key</returns>
        bool Save<T>(T instance, out object key) where T : class, new();

        /// <summary>
        ///     Save a sub-class under a base class table definition
        /// </summary>
        /// <typeparam name="T">The table type</typeparam>
        /// <param name="instance">The instance or sub-class of the table type</param>
        /// <returns></returns>
        bool SaveAs<T>(T instance, out object key) where T : class, new();

        /// <summary>
        ///     Save when key is not known
        /// </summary>
        /// <param name="actualType">The type of instance to save</param>
        /// <param name="tableType">The type used to find the table to save to</param>
        /// <param name="instance">The instance</param>
        /// <param name="cache">The cycle cache</param>
        /// <returns>The key</returns>
        bool Save(Type actualType, Type tableType, object instance, CycleCache cache, out object key);

        /// <summary>
        ///     Save when key is not known
        /// </summary>
        /// <param name="type">The type to save</param>
        /// <param name="instance">The instance</param>
        /// <returns>The key</returns>
        bool Save(Type type, object instance, out object key);

        /// <summary>
        ///     Save when key is not known
        /// </summary>
        /// <param name="type">The table type to save against</param>
        /// <param name="instance">The instance</param>
        /// <returns>The key</returns>
        bool SaveAs(Type type, object instance, out object key);

        /// <summary>
        ///     Save asynchronously
        /// </summary>
        /// <typeparam name="T">The type to save</typeparam>
        /// <param name="list">A list of items to save</param>
        /// <returns>A unique identifier for the batch</returns>
        Task<IList<object>> SaveAsync<T>(IList<T> list);
        /// <summary>
        ///     Save asynchronously
        /// </summary>
        /// <typeparam name="T">The type to save</typeparam>
        /// <param name="list">A list of items to save</param>
        /// <param name="cancellationTokenSource">CancellationTokenSource when activated cancels the save</param>
        /// <returns>A unique identifier for the batch</returns>
        Task<IList<object>> SaveAsync<T>(IList<T> list, CancellationTokenSource cancellationTokenSource);

        /// <summary>
        ///     Non-generic asynchronous save
        /// </summary>
        /// <param name="list">The list of items</param>
        /// <returns>A unique job identifier</returns>
        Task<IList<object>> SaveAsync(IList list);

        /// <summary>
        ///     Non-generic asynchronous save
        /// </summary>
        /// <param name="list">The list of items</param>
        /// <param name="cancellationTokenSource">CancellationTokenSource when activated cancels the save</param>
        /// <returns>A unique job identifier</returns>
        Task<IList<object>> SaveAsync(IList list, CancellationTokenSource cancellationTokenSource = default(CancellationTokenSource));

        /// <summary>
        ///     Flush all keys and indexes to storage
        /// </summary>
        void Flush();

        /// <summary>
        ///     Load it 
        /// </summary>
        /// <typeparam name="T">The type to load</typeparam>
        /// <typeparam name="TKey">The key type</typeparam>
        /// <param name="key">The value of the key</param>
        /// <returns>The instance</returns>
        T Load<T, TKey>(TKey key) where T : class, new();

        /// <summary>
        ///     Load it (key type not typed)
        /// </summary>
        /// <typeparam name="T">The type to load</typeparam>
        /// <param name="key">The key</param>
        /// <returns>The instance</returns>
        T Load<T>(object key) where T : class, new();

        /// <summary>
        ///     Load it without knowledge of the key type
        /// </summary>
        /// <param name="type">The type to load</param>
        /// <param name="key">The key</param>
        /// <param name="cache">The cycle cache</param>
        /// <returns>The instance</returns>
        object Load(Type type, object key, CycleCache cache);

        /// <summary>
        ///     Load it without knowledge of the key type
        /// </summary>
        /// <param name="type">The type to load</param>
        /// <param name="key">The key</param>
        /// <returns>The instance</returns>
        object Load(Type type, object key);

        /// <summary>
        ///     Delete it 
        /// </summary>
        /// <typeparam name="T">The type to delete</typeparam>
        /// <param name="instance">The instance</param>
        void Delete<T>(T instance) where T : class;

        /// <summary>
        ///     Delete it (non-generic)
        /// </summary>
        /// <param name="type">The type</param>
        /// <param name="key">The key</param>
        void Delete(Type type, object key);

        /// <summary>
        ///     Truncate all records for a type
        /// </summary>
        /// <param name="type">The type</param>
        void Truncate(Type type);

        /// <summary>
        ///     Purge the entire database - wipe it clean!
        /// </summary>
        void Purge();

        /// <summary>
        ///     Purge the entire database - wipe it clean!
        /// </summary>
        /// <param name="millisecondsTimeout">Timeout to wait for running tasks</param>
        void Purge(int millisecondsTimeout);

        /// <summary>
        ///     Purge the entire database - wipe it clean!
        /// </summary>
        /// <param name="cancellationToken">Cancellation token to halt cancelling running tasks</param>
        void Purge(CancellationToken cancellationToken);

        /// <summary>
        ///     Purge the entire database - wipe it clean!
        /// </summary>
        /// <param name="cancellationToken">Cancellation token to halt cancelling running tasks</param>
        /// <param name="millisecondsTimeout">Timeout to wait for running tasks</param>
        void Purge(CancellationToken cancellationToken, int millisecondsTimeout);


        /// <summary>
        ///     Refresh indexes and keys from disk
        /// </summary>
        void Refresh();

        /// <summary>
        ///     Event for sterling changes
        /// </summary>
        event EventHandler<SterlingOperationArgs> SterlingOperationPerformed;

        /// <summary>
        ///     Create a table definition
        /// </summary>
        /// <typeparam name="T">The type of the table</typeparam>
        /// <typeparam name="TKey">The type of the key</typeparam>
        /// <param name="keyFunction">Function to return the key</param>
        /// <returns>The table definition</returns>
        ITableDefinition CreateTableDefinition<T, TKey>(Func<T, TKey> keyFunction) where T : class, new();

        /// <summary>
        ///     Get the table definition for instance
        /// </summary>
        /// <returns>The definitions, null if none found</returns>
        ITableDefinition GetTableDefinition(object instance);

        /// <summary>
        ///     Get the list of table definitions
        /// </summary>
        /// <returns>The list of table definitions</returns>
        IEnumerable<ITableDefinition> GetTableDefinitions();

        /// <summary>
        ///     Register a new table definition
        /// </summary>
        /// <param name="tableDefinition">The new table definition</param>
        void RegisterTableDefinition(ITableDefinition tableDefinition);

        /// <summary>
        ///     Gets the property converter for the given type, or returns null if none is found.
        /// </summary>
        /// <param name="type">The type</param>
        /// <param name="propertyConverter">The property converter</param>
        /// <returns>True if there is a registered property converter.</returns>
        bool TryGetPropertyConverter(Type type, out ISterlingPropertyConverter propertyConverter);
    }
}