using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Sterling.Core.Indexes;
using Sterling.Core.Keys;

namespace Sterling.Core.Database
{
    /// <summary>
    ///     Table definnition
    /// </summary>
    public interface ITableDefinition
    {
        /// <summary>
        ///     Key list
        /// </summary>
        IKeyCollection Keys { get; }

        /// <summary>
        ///     Get a new dictionary (creates the generic)
        /// </summary>
        /// <returns>The new dictionary instance</returns>
        IDictionary GetNewDictionary();

        /// <summary>
        ///     Indexes
        /// </summary>
        ConcurrentDictionary<string, IIndexCollection> Indexes { get; }

        /// <summary>
        ///     Table type
        /// </summary>
        Type TableType { get; }

        /// <summary>
        ///     Key type
        /// </summary>
        Type KeyType { get; }

        /// <summary>
        ///     Refresh key list
        /// </summary>
        void Refresh();

        /// <summary>
        ///     Fetch the key for the instance
        /// </summary>
        /// <param name="instance">The instance</param>
        /// <returns>The key</returns>
        object FetchKeyFromInstance(object instance);

        /// <summary>
        ///     Is the instance dirty?
        /// </summary>
        /// <returns>True if dirty</returns>
        bool IsDirty(object instance);
    }
}