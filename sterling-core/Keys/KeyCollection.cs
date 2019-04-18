using System;
using System.Collections;
using System.Collections.Generic;

namespace Sterling.Core.Keys
{
    /// <summary>
    ///     Collection of keys for a given entity
    /// </summary>
    internal class KeyCollection<T, TKey> : IKeyCollection where T : class, new()
    {
        private readonly Func<TKey, T> _resolver;
        private readonly ISterlingDriver _driver;

        /// <summary>
        ///     Set when keys change
        /// </summary>
        public bool IsDirty { get; private set; }

        /// <summary>
        ///     Initialize the key collection
        /// </summary>
        /// <param name="driver">Driver</param>
        /// <param name="resolver">The resolver for loading the object</param>
        public KeyCollection(ISterlingDriver driver, Func<TKey, T> resolver)
        {
            this._driver = driver;
            this._resolver = resolver;
            _DeserializeKeys();
            this.IsDirty = false;
        }

        /// <summary>
        ///     The list of keys
        /// </summary>
        private readonly List<TableKey<T, TKey>> _keyList = new List<TableKey<T, TKey>>();

        /// <summary>
        ///     Map for keys in the set
        /// </summary>
        private readonly Dictionary<TKey, int> _keyMap = new Dictionary<TKey, int>();

        /// <summary>
        ///     Force to a new list so the internal one cannot be manipulated directly
        /// </summary>
        public List<TableKey<T, TKey>> Query { get { return new List<TableKey<T, TKey>>(this._keyList); } }
        public IEnumerable<TableKey<T, TKey>> EnumerableQuery { get { return this._keyList; } }

        private void _DeserializeKeys()
        {
            this._keyList.Clear();
            this._keyMap.Clear();

            var keyMap = this._driver.DeserializeKeys(typeof(T), typeof(TKey), new Dictionary<TKey, int>()) ?? new Dictionary<TKey, int>();

            if (keyMap.Count > 0)
            {
                foreach (var key in keyMap.Keys)
                {
                    var idx = (int)keyMap[key];
                    if (idx >= this.NextKey)
                    {
                        this.NextKey = idx + 1;
                    }
                    this._keyMap.Add((TKey)key, idx);
                    this._keyList.Add(new TableKey<T, TKey>((TKey)key, this._resolver));
                }
            }
            else
            {
                this.NextKey = 0;
            }
        }

        /// <summary>
        ///     Serializes the key list
        /// </summary>
        private void _SerializeKeys()
        {
            this._driver.SerializeKeys(typeof(T), typeof(TKey), this._keyMap);
        }

        /// <summary>
        ///     The next key
        /// </summary>
        internal int NextKey { get; private set; }

        /// <summary>
        ///     Serialize
        /// </summary>
        public void Flush()
        {
            lock (((ICollection)this._keyList).SyncRoot)
            {
                if (this.IsDirty)
                {
                    _SerializeKeys();
                }
                this.IsDirty = false;
            }
        }

        /// <summary>
        ///     Get the index for a key
        /// </summary>
        /// <param name="key">The key</param>
        /// <returns>The index</returns>
        public int GetIndexForKey(object key)
        {
            return this._keyMap.ContainsKey((TKey)key) ? this._keyMap[(TKey)key] : -1;
        }

        /// <summary>
        ///     Refresh the list
        /// </summary>
        public void Refresh()
        {
            lock (((ICollection)this._keyList).SyncRoot)
            {
                if (this.IsDirty)
                {
                    _SerializeKeys();
                }
                _DeserializeKeys();
                this.IsDirty = false;
            }
        }

        /// <summary>
        ///     Truncate the collection
        /// </summary>
        public void Truncate()
        {
            this.IsDirty = false;
            Refresh();
        }

        /// <summary>
        ///     Add a key to the list
        /// </summary>
        /// <param name="key">The key</param>
        public int AddKey(object key)
        {
            lock (((ICollection)this._keyList).SyncRoot)
            {
                var newKey = new TableKey<T, TKey>((TKey)key, this._resolver);

                if (!this._keyList.Contains(newKey))
                {
                    this._keyList.Add(newKey);
                    this._keyMap.Add((TKey)key, this.NextKey++);
                    this.IsDirty = true;
                }
                else
                {
                    var idx = this._keyList.IndexOf(newKey);
                    this._keyList[idx].Refresh();
                }
            }

            return this._keyMap[(TKey)key];
        }

        /// <summary>
        ///     Remove a key from the list
        /// </summary>
        /// <param name="key">The key</param>
        public void RemoveKey(object key)
        {
            lock (((ICollection)this._keyList).SyncRoot)
            {
                var checkKey = new TableKey<T, TKey>((TKey)key, this._resolver);

                if (!this._keyList.Contains(checkKey)) return;
                this._keyList.Remove(checkKey);
                this._keyMap.Remove((TKey)key);
                this.IsDirty = true;
            }
        }
    }
}
