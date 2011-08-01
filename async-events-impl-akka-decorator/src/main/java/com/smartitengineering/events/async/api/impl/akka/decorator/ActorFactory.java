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
package com.smartitengineering.events.async.api.impl.akka.decorator;

import akka.actor.Actors;
import akka.actor.Supervisor;
import akka.actor.SupervisorFactory;
import akka.config.Supervision;
import akka.config.Supervision.OneForOneStrategy;
import akka.config.Supervision.Supervise;
import akka.config.Supervision.SuperviseTypedActor;
import akka.config.TypedActorConfigurator;
import com.google.inject.Module;
import com.smartitengineering.events.async.api.EventPublisher;
import com.smartitengineering.util.bean.PropertiesLocator;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

/**
 *
 * @author imyousuf
 */
public final class ActorFactory {

  private final TypedActorConfigurator manager;
  private final String remoteServiceId;
  private final String remoteAddress;
  private final Integer remotePort;
  private final boolean remote;
  private final SupervisorFactory supervisorFactory;
  private final Supervisor supervisor;
  private static final String CONFIG_PATH =
                              "com/smartitengineering/events/async/api/impl/akka/decorator/decoratorconfig.properties";
  private static final String MODULE_CLASS = "moduleClass";
  private static final String RETRY_ATTEMPTS = "retryAttempts";
  private static final String RETRY_TIMEOUT = "retryTimeout";
  private static final String SUPERVISION_TIMEOUT = "supervisionTimeout";
  private static final String REMOTING = "remoting";
  private static final String REMOTE_ADDR = "remoteAddress";
  private static final String REMOTE_PORT = "remotePort";
  private static final String REMOTE_SERVICE_ID = "serviceId";

  public ActorFactory() {
    final Properties properties = loadProperties(CONFIG_PATH);
    final int retryAttempts = NumberUtils.toInt(properties.getProperty(RETRY_ATTEMPTS), 10);
    final int retryTimeout = NumberUtils.toInt(properties.getProperty(RETRY_TIMEOUT), 2000);
    final int supervisionTimeout = NumberUtils.toInt(properties.getProperty(SUPERVISION_TIMEOUT), 2000);
    if (Boolean.parseBoolean(properties.getProperty(REMOTING))) {
      remote = true;
      manager = null;
      remoteServiceId = properties.getProperty(REMOTE_SERVICE_ID);
      remoteAddress = properties.getProperty(REMOTE_ADDR);
      remotePort = NumberUtils.toInt(properties.getProperty(REMOTE_PORT), 30000);
    }
    else {
      remote = false;
      manager = new TypedActorConfigurator();
      manager.configure(new OneForOneStrategy(new Class[]{EventPublicationException.class}, retryAttempts, retryTimeout),
                        new SuperviseTypedActor[]{new SuperviseTypedActor(EventPublisher.class,
                                                                          EventPublisherActor.class,
                                                                          Supervision.permanent(), supervisionTimeout)});
      final String moduleClassStr = properties.getProperty(MODULE_CLASS);
      if (StringUtils.isNotBlank(moduleClassStr)) {
        Module module = null;
        final Class clazz;
        try {
          clazz = Class.forName(StringUtils.trim(moduleClassStr), true, Thread.currentThread().getContextClassLoader());
        }
        catch (ClassNotFoundException ex) {
          throw new IllegalStateException(ex);
        }
        if (!Module.class.isAssignableFrom(clazz)) {
          throw new IllegalArgumentException("Specified class not instance of Module");
        }
        Class<? extends Module> moduleClass = clazz;
        boolean foundConstructor = false;
        Constructor<? extends Module> defaultContructor;
        try {
          defaultContructor = moduleClass.getConstructor();
          module = defaultContructor.newInstance();
          foundConstructor = true;
        }
        catch (InstantiationException ex) {
          throw new IllegalStateException(ex);
        }
        catch (IllegalAccessException ex) {
          throw new IllegalStateException(ex);
        }
        catch (InvocationTargetException ex) {
          throw new IllegalStateException(ex);
        }
        catch (NoSuchMethodException ex) {
        }
        catch (SecurityException ex) {
        }
        if (!foundConstructor) {
          try {
            defaultContructor = moduleClass.getConstructor(Properties.class);
            module = defaultContructor.newInstance(properties);
            foundConstructor = true;
          }
          catch (InstantiationException ex) {
            throw new IllegalStateException(ex);
          }
          catch (IllegalAccessException ex) {
            throw new IllegalStateException(ex);
          }
          catch (InvocationTargetException ex) {
            throw new IllegalStateException(ex);
          }
          catch (NoSuchMethodException ex) {
          }
          catch (SecurityException ex) {
          }
        }
        if (!foundConstructor) {
          throw new IllegalStateException("No supported contructors found - no args and with a properties obj!");
        }
        manager.addExternalGuiceModule(module).inject();
      }
      manager.supervise();
      remoteServiceId = null;
      remoteAddress = null;
      remotePort = null;
    }
    /*
     * Bootstrap
     */
    {
      supervisorFactory =
      new SupervisorFactory(new Supervision.SupervisorConfig(new OneForOneStrategy(new Class[]{
            EventPublicationException.class}, retryAttempts, retryTimeout),
                                                             new Supervise[]{new Supervise(Actors.actorOf(
            EventPublisherUntypedActor.class), Supervision.permanent())}));
      supervisor = supervisorFactory.newInstance();
      supervisor.start();
    }
  }

  public EventPublisher getInstance() {
    if (remote) {
      return new EventPublisher() {

        public boolean publishEvent(String eventContentType, String eventMessage) {
          EventMessage message = new EventMessage(eventContentType, eventMessage);
          final Object reply = Actors.remote().actorFor(remoteServiceId, remoteAddress, remotePort).sendRequestReply(
              message);
          if (reply == null) {
            return false;
          }
          return Boolean.parseBoolean(reply.toString());
        }
      };
    }
    else {
      return manager.getInstance(EventPublisher.class);
    }
  }

  protected final Properties loadProperties(String propFile) throws IllegalArgumentException, IllegalStateException {
    if (StringUtils.isBlank(propFile)) {
      throw new IllegalArgumentException("Properties file location can not be blank!");
    }
    PropertiesLocator propertiesLocator = new PropertiesLocator();
    propertiesLocator.setSmartLocations(propFile);
    Properties properties = new Properties();
    try {
      propertiesLocator.loadProperties(properties);
    }
    catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
    return properties;
  }
  private static ActorFactory factory;
  private static final Semaphore semaphore = new Semaphore(1);

  public static EventPublisher getActorRef() {
    if (factory == null) {
      try {
        semaphore.acquire();
      }
      catch (Exception ex) {
        throw new IllegalArgumentException(ex);
      }
      if (factory == null) {
        factory = new ActorFactory();
      }
      semaphore.release();
    }
    return factory.getInstance();
  }
}
