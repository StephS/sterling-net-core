using Sterling.Core.Database;
using Sterling.Core.Serialization;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;

namespace Sterling.Core
{
    /// <summary>
    ///     Provides the storage driver
    /// </summary>
    public interface ISterlingDriver
    {
        SerializationHelper Helper { get; }

        /// <summary>
        ///     Name of the database the driver is registered to
        /// </summary>
        ISterlingDatabaseInstance Database { get; set; }

        /// <summary>
        ///     Logger
        /// </summary>
        Action<SterlingLogLevel, string, Exception> Log { get; set; }

        /// <summary>
        ///     The registered serializer for the database
        /// </summary>
        ISterlingSerializer DatabaseSerializer { get; set; }

        /// <summary>
        ///     Serialize the keys
        /// </summary>
        /// <param name="tableType">Type of the parent table</param>
        /// <param name="keyType">Type of the key</param>
        /// <param name="keyMap">Key map</param>
        void SerializeKeys(Type tableType, Type keyType, IDictionary keyMap);

        /// <summary>
        ///     Deserialize keys without generics
        /// </summary>
        /// <param name="tableType">The type</param>
        /// <param name="keyType">Type of the key</param>
        /// <param name="template">The template</param>
        /// <returns>The keys without the template</returns>
        IDictionary DeserializeKeys(Type tableType, Type keyType, IDictionary template);

        /// <summary>
        ///     Serialize a single index 
        /// </summary>
        /// <typeparam name="TKey">The type of the key</typeparam>
        /// <typeparam name="TIndex">The type of the index</typeparam>
        /// <param name="tableType">The type of the parent table</param>
        /// <param name="indexName">The name of the index</param>
        /// <param name="indexMap">The index map</param>
        void SerializeIndex<TKey, TIndex>(Type tableType, string indexName, ConcurrentDictionary<TKey, TIndex> indexMap);

        /// <summary>
        ///     Serialize a double index 
        /// </summary>
        /// <typeparam name="TKey">The type of the key</typeparam>
        /// <typeparam name="TIndex1">The type of the first index</typeparam>
        /// <typeparam name="TIndex2">The type of the second index</typeparam>
        /// <param name="tableType">The type of the parent table</param>
        /// <param name="indexName">The name of the index</param>
        /// <param name="indexMap">The index map</param>        
        void SerializeIndex<TKey, TIndex1, TIndex2>(Type tableType, string indexName, ConcurrentDictionary<TKey, Tuple<TIndex1, TIndex2>> indexMap);

        /// <summary>
        ///     Deserialize a single index
        /// </summary>
        /// <typeparam name="TKey">The type of the key</typeparam>
        /// <typeparam name="TIndex">The type of the index</typeparam>
        /// <param name="tableType">The type of the parent table</param>
        /// <param name="indexName">The name of the index</param>        
        /// <returns>The index map</returns>
        ConcurrentDictionary<TKey, TIndex> DeserializeIndex<TKey, TIndex>(Type tableType, string indexName);

        /// <summary>
        ///     Deserialize a double index
        /// </summary>
        /// <typeparam name="TKey">The type of the key</typeparam>
        /// <typeparam name="TIndex1">The type of the first index</typeparam>
        /// <typeparam name="TIndex2">The type of the second index</typeparam>
        /// <param name="tableType">The type of the parent table</param>
        /// <param name="indexName">The name of the index</param>        
        /// <returns>The index map</returns>        
        ConcurrentDictionary<TKey, Tuple<TIndex1, TIndex2>> DeserializeIndex<TKey, TIndex1, TIndex2>(Type tableType, string indexName);

        /// <summary>
        ///     Publish the list of tables
        /// </summary>
        /// <param name="tables">The list of tables</param>
        void PublishTables(ConcurrentDictionary<Type, ITableDefinition> tables);

        /// <summary>
        ///     Serialize the type master
        /// </summary>
        void SerializeTypes();

        /// <summary>
        ///     Deserialize the type master
        /// </summary>
        /// <param name="types">The list of types</param>
        void DeserializeTypes(IList<string> types);

        /// <summary>
        ///     Get the type master
        /// </summary>
        /// <returns></returns>
        IList<string> GetTypes();

        /// <summary>
        ///     Get the index for the type
        /// </summary>
        /// <param name="type">The type</param>
        /// <returns>The type</returns>
        int GetTypeIndex(string type);

        /// <summary>
        ///     Get the type at an index
        /// </summary>
        /// <param name="index">The index</param>
        /// <returns>The type</returns>
        string GetTypeAtIndex(int index);

        /// <summary>
        /// Registers the byte stream interceptor
        /// </summary>
        /// <typeparam name="T">The interceptor</typeparam>
        void RegisterInterceptor<T>() where T : BaseSterlingByteInterceptor, new();

        /// <summary>
        ///     Unregister a byte stream interceptor
        /// </summary>
        /// <typeparam name="T">The interceptor</typeparam>
        void UnRegisterInterceptor<T>() where T : BaseSterlingByteInterceptor, new();

        /// <summary>
        /// Clears the byte stream interceptor list
        /// </summary>
        void UnRegisterInterceptors();

        /// <summary>
        ///     Save operation
        /// </summary>
        /// <param name="tableType">Type of the parent</param>
        /// <param name="keyIndex">Index for the key</param>
        /// <param name="bytes">The byte stream</param>
        void Save(Type tableType, int keyIndex, byte[] bytes);

        /// <summary>
        ///     Save operation
        /// </summary>
        /// <param name="actualType">The actual Type of the parent</param>
        /// <param name="tableType">Type of the parent table</param>
        /// <param name="instance">The object instance</param>
        /// <param name="keyIndex">Index for the key</param>
        /// <param name="cache">The cache</param>
        void Save(Type actualType, Type tableType, object instance, int keyIndex, CycleCache cache);

        /// <summary>
        ///     Load from the store
        /// </summary>
        /// <param name="tableType">The type of the parent</param>
        /// <param name="keyIndex">The index of the key</param>
        /// <returns>The byte stream</returns>
        BinaryReader Load(Type tableType, int keyIndex);

        /// <summary>
        ///     Load from the store
        /// </summary>
        /// <param name="tableType">The type of the parent</param>
        /// <param name="key">the key to the instance</param>
        /// <param name="keyIndex">The index of the key</param>
        /// <param name="cache">The cycle cache</param>
        /// <returns>The byte stream</returns>
        object Load(Type tableType, object key, int keyIndex, CycleCache cache);

        /// <summary>
        ///     Delete from the store
        /// </summary>
        /// <param name="type">The type of the parent</param>
        /// <param name="keyIndex">The index of the key</param>
        void Delete(Type type, int keyIndex);

        /// <summary>
        ///     Truncate a type
        /// </summary>
        /// <param name="type">The type to truncate</param>
        void Truncate(Type type);

        /// <summary>
        ///     Purge the database
        /// </summary>
        void Purge();

    }
}
