using Sterling.Core.Database;
using Sterling.Core.Serialization;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Sterling.Core
{
    /// <summary>
    ///     Default in-memory driver
    /// </summary>
    public class MemoryDriver : BaseDriver
    {
        public MemoryDriver()
        {
        }

        public MemoryDriver(ISterlingDatabaseInstance database, ISterlingSerializer serializer, Action<SterlingLogLevel, string, Exception> log) : base(database, serializer, log)
        {
        }

        /// <summary>
        /// The byte stream interceptor list. 
        /// </summary>
        private readonly List<BaseSterlingByteInterceptor> _byteInterceptorList =
            new List<BaseSterlingByteInterceptor>();

        /// <summary>
        /// Registers the BaseSterlingByteInterceptor
        /// </summary>
        /// <typeparam name="T">The type of the interceptor</typeparam>
        public override void RegisterInterceptor<T>()
        {
            this._byteInterceptorList.Add((T)Activator.CreateInstance(typeof(T)));
        }

        public override void UnRegisterInterceptor<T>()
        {
            var interceptor = (from i
                                   in this._byteInterceptorList
                               where i.GetType().Equals(typeof(T))
                               select i).FirstOrDefault();

            if (interceptor != null)
            {
                this._byteInterceptorList.Remove(interceptor);
            }
        }

        /// <summary>
        /// Clears the _byteInterceptorList object
        /// </summary>
        public override void UnRegisterInterceptors()
        {
            if (this._byteInterceptorList != null)
            {
                this._byteInterceptorList.Clear();
            }
        }

        /// <summary>
        ///     Keys
        /// </summary>
        private readonly ConcurrentDictionary<Type, object> _keyCache = new ConcurrentDictionary<Type, object>();

        /// <summary>
        ///     Indexes
        /// </summary>
        private readonly ConcurrentDictionary<Type, ConcurrentDictionary<string, object>> _indexCache = new ConcurrentDictionary<Type, ConcurrentDictionary<string, object>>();

        /// <summary>
        ///     Objects
        /// </summary>
        private readonly ConcurrentDictionary<Tuple<string, int>, byte[]> _objectCache = new ConcurrentDictionary<Tuple<string, int>, byte[]>();

        private SerializationHelper _serializationHelper;

        public SerializationHelper serializationHelper
        {
            get
            {
                return this._serializationHelper ?? (this._serializationHelper = new SerializationHelper(this.Database, this.DatabaseSerializer, SterlingFactory.GetLogger(),
                                                                    s => GetTypeIndex(s),
                                                                    i => GetTypeAtIndex(i)));
            }
        }

        /// <summary>
        ///     Serialize the keys
        /// </summary>
        /// <param name="type">Type of the parent table</param>
        /// <param name="keyType">Type of the key</param>
        /// <param name="keyMap">Key map</param>
        public override void SerializeKeys(Type type, Type keyType, IDictionary keyMap)
        {
            //lock (((ICollection)_keyCache).SyncRoot)
            //{
            this._keyCache[type] = keyMap;
            //}
        }

        /// <summary>
        ///     Deserialize keys without generics
        /// </summary>
        /// <param name="type">The type</param>
        /// <param name="keyType">Type of the key</param>
        /// <param name="template">The template</param>
        /// <returns>The keys without the template</returns>
        public override IDictionary DeserializeKeys(Type type, Type keyType, IDictionary template)
        {
            return this._keyCache.ContainsKey(type) ? this._keyCache[type] as IDictionary : template;
        }

        /// <summary>
        ///     Serialize a single index 
        /// </summary>
        /// <typeparam name="TKey">The type of the key</typeparam>
        /// <typeparam name="TIndex">The type of the index</typeparam>
        /// <param name="type">The type of the parent table</param>
        /// <param name="indexName">The name of the index</param>
        /// <param name="indexMap">The index map</param>
        public override void SerializeIndex<TKey, TIndex>(Type type, string indexName, ConcurrentDictionary<TKey, TIndex> indexMap)
        {
            var indexCache = this._indexCache.GetOrAdd(type, x => new ConcurrentDictionary<string, object>());

            //var indexCache = _indexCache[type];

            indexCache[indexName] = indexMap;
        }

        /// <summary>
        ///     Serialize a double index 
        /// </summary>
        /// <typeparam name="TKey">The type of the key</typeparam>
        /// <typeparam name="TIndex1">The type of the first index</typeparam>
        /// <typeparam name="TIndex2">The type of the second index</typeparam>
        /// <param name="type">The type of the parent table</param>
        /// <param name="indexName">The name of the index</param>
        /// <param name="indexMap">The index map</param>        
        public override void SerializeIndex<TKey, TIndex1, TIndex2>(Type type, string indexName, ConcurrentDictionary<TKey, Tuple<TIndex1, TIndex2>> indexMap)
        {
            var indexCache = this._indexCache.GetOrAdd(type, x => new ConcurrentDictionary<string, object>());

            //var indexCache = _indexCache[type];

            indexCache[indexName] = indexMap;
            /*

            lock (((ICollection)_indexCache).SyncRoot)
            {
                if (!_indexCache.ContainsKey(type))
                {
                    _indexCache.Add(type, new ConcurrentDictionary<string, object>());
                }

                var indexCache = _indexCache[type];
                indexCache[indexName] = indexMap;
            }
            */
        }

        /// <summary>
        ///     Deserialize a single index
        /// </summary>
        /// <typeparam name="TKey">The type of the key</typeparam>
        /// <typeparam name="TIndex">The type of the index</typeparam>
        /// <param name="type">The type of the parent table</param>
        /// <param name="indexName">The name of the index</param>        
        /// <returns>The index map</returns>
        public override ConcurrentDictionary<TKey, TIndex> DeserializeIndex<TKey, TIndex>(Type type, string indexName)
        {
            if (this._indexCache.TryGetValue(type, out ConcurrentDictionary<string, object> indexCache))
            {
                if (indexCache.TryGetValue(indexName, out object result))
                {
                    return result as ConcurrentDictionary<TKey, TIndex>;
                }
            }
            return null;
        }

        /// <summary>
        ///     Deserialize a double index
        /// </summary>
        /// <typeparam name="TKey">The type of the key</typeparam>
        /// <typeparam name="TIndex1">The type of the first index</typeparam>
        /// <typeparam name="TIndex2">The type of the second index</typeparam>
        /// <param name="type">The type of the parent table</param>
        /// <param name="indexName">The name of the index</param>        
        /// <returns>The index map</returns>        
        public override ConcurrentDictionary<TKey, Tuple<TIndex1, TIndex2>> DeserializeIndex<TKey, TIndex1, TIndex2>(Type type, string indexName)
        {
            if (this._indexCache.TryGetValue(type, out ConcurrentDictionary<string, object> indexCache))
            {
                if (indexCache.TryGetValue(indexName, out object result))
                {
                    return result as ConcurrentDictionary<TKey, Tuple<TIndex1, TIndex2>>;
                }
            }
            return null;
        }

        /// <summary>
        ///     Publish the list of tables
        /// </summary>
        /// <param name="tables">The list of tables</param>
        public override void PublishTables(ConcurrentDictionary<Type, ITableDefinition> tables)
        {
            return;
        }

        /// <summary>
        ///     Serialize the type master
        /// </summary>
        public override void SerializeTypes()
        {
            return;
        }

        /// <summary>
        ///     Save operation
        /// </summary>
        /// <param name="tableType">Type of the parent</param>
        /// <param name="keyIndex">Index for the key</param>
        /// <param name="bytes">The byte stream</param>
        public override void Save(Type tableType, int keyIndex, byte[] bytes)
        {
            var key = Tuple.Create(tableType.FullName, keyIndex);
            this._objectCache[key] = bytes;
        }

        /// <summary>
        ///     Load from the store
        /// </summary>
        /// <param name="tableType">The type of the parent</param>
        /// <param name="keyIndex">The index of the key</param>
        /// <returns>The byte stream</returns>
        public override BinaryReader Load(Type tableType, int keyIndex)
        {
            var key = Tuple.Create(tableType.FullName, keyIndex);
            byte[] bytes;

            bytes = this._objectCache[key];

            var memStream = new MemoryStream(bytes);
            return new BinaryReader(memStream);
        }

        /// <summary>
        ///     Delete from the store
        /// </summary>
        /// <param name="type">The type of the parent</param>
        /// <param name="keyIndex">The index of the key</param>
        public override void Delete(Type type, int keyIndex)
        {
            var key = Tuple.Create(type.FullName, keyIndex);

            this._objectCache.TryRemove(key, out byte[] throwaway);
        }

        /// <summary>
        ///     Truncate a type
        /// </summary>
        /// <param name="type">The type to truncate</param>
        public override void Truncate(Type type)
        {
            var typeString = type.FullName;

            var keys = from key in this._objectCache.Keys where key.Item1.Equals(typeString) select key;

            byte[] throwawaybyte;
            foreach (var key in keys.ToList())
            {
                this._objectCache.TryRemove(key, out throwawaybyte);
            }

            var indexes = from index in this._indexCache.Keys where this._indexCache.ContainsKey(type) select index;

            ConcurrentDictionary<string, object> ThrowawayCD;

            foreach (var index in indexes.ToList())
            {
                this._indexCache.TryRemove(index, out ThrowawayCD);
            }

            this._keyCache.TryRemove(type, out object throwaway);

        }

        /// <summary>
        ///     Purge the database
        /// </summary>
        public override void Purge()
        {
            var types = from key in this._keyCache.Keys select key;
            foreach (var type in types.ToList())
            {
                Truncate(type);
            }
        }

        public override void Save(Type actualType, Type tableType, object instance, int keyIndex, CycleCache cache)
        {
            var memStream = new MemoryStream();

            try
            {
                using (var bw = new BinaryWriter(memStream))
                {
                    this.serializationHelper.Save(actualType, instance, bw, cache, true);

                    bw.Flush();

                    if (this._byteInterceptorList.Count > 0)
                    {
                        var bytes = memStream.GetBuffer();

                        bytes = this._byteInterceptorList.Aggregate(bytes,
                                                               (current, byteInterceptor) =>
                                                               byteInterceptor.Save(current));

                        memStream = new MemoryStream(bytes);
                    }

                    memStream.Seek(0, SeekOrigin.Begin);
                    this.Save(tableType, keyIndex, memStream.ToArray());

                }
            }
            finally
            {
                memStream.Flush();
                memStream.Close();
            }
        }

        public override object Load(Type tableType, object key, int keyIndex, CycleCache cache)
        {
            BinaryReader br = null;
            MemoryStream memStream = null;

            try
            {
                br = this.Load(tableType, keyIndex);

                //var serializationHelper = new SerializationHelper(this.Database, this.DatabaseSerializer, SterlingFactory.GetLogger(),
                //                                                  s => this.GetTypeIndex(s),
                //                                                  i => this.GetTypeAtIndex(i));
                if (this._byteInterceptorList.Count > 0)
                {
                    var bytes = br.ReadBytes((int)br.BaseStream.Length);

                    bytes = this._byteInterceptorList.ToArray().Reverse().Aggregate(bytes,
                                                                               (current, byteInterceptor) =>
                                                                               byteInterceptor.Load(current));

                    memStream = new MemoryStream(bytes);

                    br.Close();
                    br = new BinaryReader(memStream);
                }
                return serializationHelper.Load(tableType, key, br, cache);
            }
            finally
            {
                if (br != null)
                {
                    br.Close();
                }

                if (memStream != null)
                {
                    memStream.Flush();
                    memStream.Close();
                }
            }
        }
    }
}