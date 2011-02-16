/*
 *
 * This is a framework for Asynchronous Event processing based on event hub.
 * Copyright (C) 2011  Imran M Yousuf (imyousuf@smartitengineering.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.smartitengineering.events.async.api.impl.hub;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.smartitengineering.events.async.api.EventConsumer;
import com.smartitengineering.events.async.api.EventPublisher;
import com.smartitengineering.events.async.api.EventSubscriber;
import com.smartitengineering.util.bean.guice.GuiceUtil;
import com.smartitengineering.util.rest.client.ApplicationWideClientFactoryImpl;
import com.smartitengineering.util.rest.client.ConnectionConfig;
import com.smartitengineering.util.rest.client.jersey.cache.CacheableClient;
import com.sun.jersey.api.client.Client;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang.mutable.MutableInt;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test for simple App.
 */
public class AppTest {

  private static final int PORT = 10080;
  private static final Logger LOGGER = LoggerFactory.getLogger(AppTest.class);
  private static Server jettyServer;
  private static Injector injector;
  private static EventPublisher publisher;
  private static EventSubscriber subscriber;

  @BeforeClass
  public static void globalSetup() throws Exception {

    /*
     * Start web application container
     */
    jettyServer = new Server(PORT);
    HandlerList handlerList = new HandlerList();
    /*
     * The following is for solr for later, when this is to be used it
     */
    Handler solr = new WebAppContext("./target/hub/", "/hub");
    handlerList.addHandler(solr);
    jettyServer.setHandler(handlerList);
    jettyServer.setSendDateHeader(true);
    jettyServer.start();

    /*
     * Setup client properties
     */
    System.setProperty(ApplicationWideClientFactoryImpl.TRACE, "true");

    Client client = CacheableClient.create();
    client.resource("http://localhost:10080/hub/api/channels/test").header(HttpHeaders.CONTENT_TYPE,
                                                                           MediaType.APPLICATION_JSON).put(
        "{\"name\":\"test\"}");
    LOGGER.info("Created test channel!");
    /*
     * Ensure DIs done
     */
    Properties properties = new Properties();
    properties.setProperty(GuiceUtil.CONTEXT_NAME_PROP, "com.smartitengineering");
    properties.setProperty(GuiceUtil.IGNORE_MISSING_DEP_PROP, Boolean.TRUE.toString());
    properties.setProperty(GuiceUtil.MODULES_LIST_PROP, ConfigurationModule.class.getName());
    final GuiceUtil instance = GuiceUtil.getInstance(properties);
    instance.register();
    LOGGER.info("Register injectors!");
    /*
     * Initialize injector
     */
    injector = instance.getInjectors()[0];
    publisher = injector.getInstance(EventPublisher.class);
    subscriber = injector.getInstance(EventSubscriber.class);
    LOGGER.info("Initialize publisher and subscriber!");
  }

  @AfterClass
  public static void globalTearDown() throws Exception {
    jettyServer.stop();
  }

  @Test
  public void testApp() throws Exception {
    final String textPlain = "text/plain";
    final String message = "Message";
    final MutableInt mutableInt = new MutableInt();
    EventConsumer consumer = new EventConsumer() {

      @Override
      public void consume(String eventContentType, String eventMessage) {
        Assert.assertEquals(textPlain, eventContentType);
        Assert.assertEquals(message, eventMessage);
        mutableInt.add(1);
      }
    };
    subscriber.addConsumer(consumer);
    publisher.publishEvent(textPlain, message);
    LOGGER.info("Publish first event!");
    Thread.sleep(1200);
    Assert.assertEquals(1, mutableInt.intValue());
    publisher.publishEvent(textPlain, message);
    LOGGER.info("Publish second event!");
    Thread.sleep(1200);
    Assert.assertEquals(2, mutableInt.intValue());
    subscriber.removeConsumer(consumer);

  }

  public static class ConfigurationModule extends AbstractModule {

    @Override
    protected void configure() {
      ConnectionConfig config = new ConnectionConfig();
      config.setBasicUri("");
      config.setContextPath("/hub/");
      config.setHost("localhost");
      config.setPort(PORT);
      bind(ConnectionConfig.class).toInstance(config);
      bind(String.class).annotatedWith(Names.named("channelHubUri")).toInstance(
          "http://localhost:10080/hub/api/channels/test/hub");
      bind(String.class).annotatedWith(Names.named("eventAtomFeedUri")).toInstance(
          "http://localhost:10080/hub/api/channels/test/events");
      bind(String.class).annotatedWith(Names.named("subscribtionCronExpression")).toInstance("0/1 * * * * ?");
      bind(new TypeLiteral<List<EventConsumer>>() {
      }).toInstance(Collections.<EventConsumer>emptyList());
      bind(EventPublisher.class).to(EventPublisherImpl.class);
      bind(EventSubscriber.class).to(EventSubscriberImpl.class);
    }
  }
}
