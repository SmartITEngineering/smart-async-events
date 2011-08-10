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

import com.smartitengineering.events.async.api.EventPublicationException;
import akka.config.Supervision;
import akka.config.Supervision.OneForOneStrategy;
import akka.config.Supervision.SuperviseTypedActor;
import akka.config.TypedActorConfigurator;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.smartitengineering.events.async.api.EventPublisher;
import junit.framework.TestCase;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.Sequence;
import org.jmock.integration.junit3.JUnit3Mockery;

/**
 *
 * @author imyousuf
 */
public class FaultTolerantTypedActorTest extends TestCase {

  private static final Mockery mockery = new JUnit3Mockery();
  private static final EventPublisher mock = mockery.mock(EventPublisher.class);

  @Override
  public void setUp() {
    mockery.checking(new Expectations() {

      {
        Sequence sequence = mockery.sequence("name");
        exactly(3).of(mock).publishEvent("a", "a");
        will(throwException(new EventPublicationException(new NullPointerException())));
        inSequence(sequence);
        exactly(1).of(mock).publishEvent("a", "a");
        will(returnValue(true));
        inSequence(sequence);
      }
    });
  }

  public void testTypedFaultTolerance() {
    TypedActorConfigurator manager;
    manager = new TypedActorConfigurator();
    manager = manager.configure(new OneForOneStrategy(new Class[]{EventPublicationException.class}, 10, 2000),
                                new SuperviseTypedActor[]{new SuperviseTypedActor(EventPublisher.class,
                                                                                  EventPublisherActor.class,
                                                                                  Supervision.permanent(), 2000)}).
        addExternalGuiceModule(new Module()).inject().supervise();
    final EventPublisher instance = manager.getInstance(EventPublisher.class);
    instance.publishEvent("a", "a");
  }

  public static class Module extends AbstractModule {

    @Override
    protected void configure() {

      bind(EventPublisher.class).annotatedWith(Names.named("decorateePublisher")).toInstance(mock);
    }
  }
}
