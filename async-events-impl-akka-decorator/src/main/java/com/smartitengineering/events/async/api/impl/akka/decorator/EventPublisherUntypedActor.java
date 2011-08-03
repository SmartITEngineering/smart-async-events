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

import akka.actor.UntypedActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author imyousuf
 */
public class EventPublisherUntypedActor extends UntypedActor {

  protected transient final Logger logger = LoggerFactory.getLogger(getClass());

  @Override
  public void onReceive(Object o) throws Exception {
    if (o instanceof EventMessage) {
      EventMessage message = (EventMessage) o;
      logger.info("Invoking from untyped actor to publish the event and passing its return-obj as unsafe message");
      getContext().replyUnsafe(Boolean.toString(ActorFactory.getActorRef().publishEvent(message.getEventType(), message.
          getMessage())));
    }
    else {
      throw new IllegalArgumentException("Unknown message!");
    }
  }
}
