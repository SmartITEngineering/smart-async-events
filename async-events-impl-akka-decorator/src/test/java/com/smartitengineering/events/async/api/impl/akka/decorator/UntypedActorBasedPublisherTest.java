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
import java.util.Properties;
import junit.framework.TestCase;
import org.jmock.Expectations;
import org.jmock.Sequence;

/**
 * Unit test for simple App.
 */
public class UntypedActorBasedPublisherTest extends TestCase {

  private ActorFactory factory;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    AppTest.mockery.checking(new Expectations() {

      {
        Sequence sequence = AppTest.mockery.sequence("untyped");
        exactly(3).of(AppTest.mock).publishEvent(with(any(String.class)), with(any(String.class)));
        will(throwException(new EventPublicationException(new NullPointerException())));
        inSequence(sequence);
        exactly(1).of(AppTest.mock).publishEvent("a", "a");
        will(returnValue(true));
        inSequence(sequence);
      }
    });

    Actors.remote().start("localhost", 30000).register("publishingService", Actors.actorOf(
        EventPublisherUntypedActor.class));
    final Properties props = ActorFactory.loadProperties(
        "com/smartitengineering/events/async/api/impl/akka/decorator/decoratorconfig.properties");
    props.setProperty("remoting", "true");
    factory = new ActorFactory(props);
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    Actors.remote().shutdown();
  }

  public void testUntypedActorBasedImplementationWithFailures() {
    factory.getInstance().publishEvent("a", "a");
    AppTest.mockery.assertIsSatisfied();
  }
}
