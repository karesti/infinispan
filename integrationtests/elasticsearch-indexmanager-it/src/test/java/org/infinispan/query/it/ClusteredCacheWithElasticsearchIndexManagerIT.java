package org.infinispan.query.it;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import org.apache.lucene.search.Query;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.distribution.ch.impl.AffinityPartitioner;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.blackbox.ClusteredCacheTest;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.Person;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

/**
 * @since 9.0
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredCacheWithElasticsearchIndexManagerIT")
public class ClusteredCacheWithElasticsearchIndexManagerIT extends ClusteredCacheTest {

    @Override
    public void testCombinationOfFilters() throws Exception {
        // Not supported by hibernate search
    }

    @Override
    public void testFullTextFilterOnOff() throws Exception {
        // Not supported by hibernate search
    }

    @Override
    public void testSearchKeyTransformer() throws Exception {
        // Will be fixed in Hibernate Search v. 5.8.0.Beta2 : see HSEARCH-2688
    }

    @Override
    protected void createCacheManagers() throws Throwable {
        ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, transactionsEnabled());
        cacheCfg.clustering().hash().keyPartitioner(new AffinityPartitioner());
        cacheCfg.indexing()
                .index(Index.LOCAL)
                .addIndexedEntity(Person.class);
        ElasticsearchTesting.applyTestProperties(cacheCfg.indexing());
        List<Cache<String, Person>> caches = createClusteredCaches(2, cacheCfg);
        cache1 = caches.get(0);
        cache2 = caches.get(1);
    }

    @AfterMethod
    @Override
    protected void clearContent() throws Throwable {
        // super.clearContent() clears the data container and the stores of all the non-private caches.
        // Invoke clear() instead to clear the indexes stored in elasticsearch.
        cache(0).clear();
    }

    public void testToto() throws Exception {
        TransactionManager transactionManager = cache2.getAdvancedCache().getTransactionManager();
        SearchManager searchManager = Search.getSearchManager(cache2);
        QueryBuilder queryBuilder = searchManager
              .buildQueryBuilderForClass(Person.class)
              .get();
        Query allQuery = queryBuilder.all().createQuery();

        String key = "newGoat";
        Person person4 = new Person(key, "eats something", 42);

        // compute a new key
        Function mappingFunction = (Function<String, Person> & Serializable) k -> new Person(k, "eats something", 42);
        if (transactionsEnabled()) transactionManager.begin();
        cache2.putIfAbsent(key, person4);
        if (transactionsEnabled()) transactionManager.commit();
        StaticTestingErrorHandler.assertAllGood(cache1, cache2);

        List<Person> found = searchManager.<Person>getQuery(allQuery, Person.class).list();
        assertEquals(1, found.size());
        assertTrue(found.contains(person4));
    }
}
