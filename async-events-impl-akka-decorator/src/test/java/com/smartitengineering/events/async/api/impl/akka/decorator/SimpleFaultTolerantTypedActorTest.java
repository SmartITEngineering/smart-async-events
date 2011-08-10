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

import akka.actor.TypedActor;
import akka.config.Supervision;
import akka.config.Supervision.OneForOneStrategy;
import akka.config.Supervision.SuperviseTypedActor;
import akka.config.TypedActorConfigurator;
import com.smartitengineering.events.async.api.EventPublisher;
import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.TestCase;

/**
 *
 * @author imyousuf
 */
public class SimpleFaultTolerantTypedActorTest extends TestCase {

  private static final AtomicInteger mutableInt = new AtomicInteger(0);

  @Override
  public void setUp() {
    mutableInt.set(0);
  }

  public static class SimpleEventPublisher extends TypedActor implements EventPublisher {

    public boolean publishEvent(String eventContentType, String eventMessage) {
      if (mutableInt.addAndGet(1) < 4) {
        throw new EventPublicationException(new NullPointerException("Custom NPE!"));
      }
      return true;
    }
  }

  public void testSimpleTypedFaultTolerance() {
    TypedActorConfigurator manager;
    manager = new TypedActorConfigurator();
    manager = manager.configure(new OneForOneStrategy(new Class[]{EventPublicationException.class}, 10, 2000),
                                new SuperviseTypedActor[]{new SuperviseTypedActor(EventPublisher.class,
                                                                                  SimpleEventPublisher.class,
                                                                                  Supervision.permanent(), 2000)}).
        supervise();
    final EventPublisher instance = manager.getInstance(EventPublisher.class);
    instance.publishEvent("a", "a");
  }
}
