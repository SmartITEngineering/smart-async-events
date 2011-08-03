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
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.smartitengineering.events.async.api.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author imyousuf
 */
public class EventPublisherActor extends TypedActor implements EventPublisher {

  @Inject
  @Named("decorateePublisher")
  private EventPublisher publisher;
  protected transient final Logger logger = LoggerFactory.getLogger(getClass());

  public boolean publishEvent(String eventContentType, String eventMessage) {
    logger.info("Invoking the injected publisher, which is fault tolerant");
    return publisher.publishEvent(eventContentType, eventMessage);
  }
}
