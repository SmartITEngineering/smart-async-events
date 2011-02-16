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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.internal.Nullable;
import com.google.inject.name.Named;
import com.smartitengineering.events.async.api.EventConsumer;
import com.smartitengineering.events.async.api.EventSubscriber;
import com.smartitengineering.util.rest.atom.AbstractFeedClientResource;
import com.smartitengineering.util.rest.atom.AtomClientUtil;
import com.smartitengineering.util.rest.client.AbstractClientResource;
import com.smartitengineering.util.rest.client.ClientUtil;
import com.smartitengineering.util.rest.client.Resource;
import com.smartitengineering.util.rest.client.ResourceLink;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.atom.abdera.impl.provider.entity.FeedProvider;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.ws.rs.core.MediaType;
import org.apache.abdera.model.Entry;
import org.apache.abdera.model.Feed;
import org.apache.abdera.model.Link;
import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerListener;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.jobs.NoOpJob;

/**
 *
 * @author imyousuf
 */
@Singleton
public class EventSubscriberImpl implements EventSubscriber {

  private final List<EventConsumer> consumers = Collections.synchronizedList(new ArrayList<EventConsumer>());
  private final String cronExpression;
  private final String eventAtomFeedUri;
  private String nextUri;

  @Inject
  public EventSubscriberImpl(@Named("subscribtionCronExpression") String cronExpression,
                             @Named("eventAtomFeedUri") String eventAtomFeedUri,
                             @Nullable List<EventConsumer> consumers) throws Exception {
    this.cronExpression = cronExpression;
    this.eventAtomFeedUri = eventAtomFeedUri;
    setInitialConsumers(consumers);
    initCronJob();
  }

  public final void setInitialConsumers(List<EventConsumer> consumers) {
    if (consumers != null && !consumers.isEmpty()) {
      this.consumers.addAll(consumers);
    }
  }

  @Override
  public void addConsumer(EventConsumer consumer) {
    consumers.add(consumer);
  }

  @Override
  public void removeConsumer(EventConsumer consumer) {
    consumers.remove(consumer);
  }

  @Override
  public void removeAllConsumers() {
    consumers.clear();
  }

  @Override
  public void poll() {
    ChannelEventsResource resource;
    boolean traverseOlder = false;
    if (nextUri == null) {
      resource = new ChannelEventsResource(ClientUtil.createResourceLink("events", URI.create(eventAtomFeedUri),
                                                                         MediaType.APPLICATION_ATOM_XML));
      traverseOlder = true;
    }
    else {
      resource = new ChannelEventsResource(ClientUtil.createResourceLink("events", URI.create(nextUri),
                                                                         MediaType.APPLICATION_ATOM_XML));
    }
    processFeed(resource, traverseOlder);
  }

  @Override
  public Collection<EventConsumer> getConsumers() {
    return Collections.unmodifiableCollection(consumers);
  }

  @Override
  public String getCronExpressionForPollSubscription() {
    return cronExpression;
  }

  private void processFeed(ChannelEventsResource resource, boolean traverseOlder) {
    final Feed lastReadStateOfEntity = resource.getLastReadStateOfEntity();
    final List<Entry> entries = lastReadStateOfEntity == null ? Collections.<Entry>emptyList() : lastReadStateOfEntity.
        getEntries();
    if (traverseOlder) {
      if (entries != null && !entries.isEmpty()) {
        final ChannelEventsResource next = resource.next();
        if (next != null) {
          processFeed(next, traverseOlder);
        }
      }
    }
    if (entries == null || entries.isEmpty()) {
      nextUri = resource.getUri().toASCIIString();
      return;
    }
    //Reverse it to get the older event first
    Collections.reverse(entries);
    for (Entry entry : entries) {
      Link altLink = entry.getAlternateLink();
      final HubEvent event = new EventResource(resource, AtomClientUtil.convertFromAtomLinkToResourceLink(altLink)).
          getLastReadStateOfEntity();
      for (final EventConsumer consumer : consumers) {
        consumer.consume(event.getContentType(), event.getContentAsString());
      }
    }
    final ChannelEventsResource previous = resource.previous();
    if (previous != null) {
      processFeed(previous, false);
    }
  }

  private void initCronJob() throws Exception {
    Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
    final CronPollListener cronPollListener = new CronPollListener();
    scheduler.addTriggerListener(cronPollListener);
    JobDetail detail = new JobDetail("pollJob", "poll", NoOpJob.class);
    Trigger trigger = new CronTrigger("pollTrigger", "poll", cronExpression);
    trigger.addTriggerListener("poll-listener");
    scheduler.start();
    scheduler.scheduleJob(detail, trigger);
  }

  public class CronPollListener implements TriggerListener {

    private AtomicBoolean atomicBoolean = new AtomicBoolean(true);

    @Override
    public String getName() {
      return "poll-listener";
    }

    @Override
    public void triggerFired(Trigger trgr, JobExecutionContext jec) {
      if (atomicBoolean.get()) {
        if (atomicBoolean.compareAndSet(true, false)) {
          poll();
          atomicBoolean.set(true);
        }
      }
    }

    @Override
    public boolean vetoJobExecution(Trigger trgr, JobExecutionContext jec) {
      return false;
    }

    @Override
    public void triggerMisfired(Trigger trgr) {
    }

    @Override
    public void triggerComplete(Trigger trgr, JobExecutionContext jec, int i) {
    }
  }

  private static class EventResource extends AbstractClientResource<HubEvent, Resource> {

    public EventResource(Resource referrer, ResourceLink resouceLink) throws IllegalArgumentException,
                                                                             UniformInterfaceException {
      super(referrer, resouceLink);
    }

    @Override
    protected void processClientConfig(ClientConfig clientConfig) {
    }

    @Override
    protected Resource instantiatePageableResource(ResourceLink link) {
      return null;
    }

    @Override
    protected ResourceLink getNextUri() {
      return null;
    }

    @Override
    protected ResourceLink getPreviousUri() {
      return null;
    }
  }

  private static class ChannelEventsResource extends AbstractFeedClientResource<ChannelEventsResource> {

    public ChannelEventsResource(ResourceLink resouceLink) throws IllegalArgumentException,
                                                                  UniformInterfaceException {
      this(null, resouceLink);
    }

    private ChannelEventsResource(Resource referrer, ResourceLink resouceLink) throws IllegalArgumentException,
                                                                                      UniformInterfaceException {
      super(referrer, resouceLink);
    }

    @Override
    protected void processClientConfig(ClientConfig clientConfig) {
      clientConfig.getClasses().add(FeedProvider.class);
      clientConfig.getClasses().add(JacksonJsonProvider.class);
    }

    @Override
    protected ChannelEventsResource instantiatePageableResource(ResourceLink link) {
      return new ChannelEventsResource(this, link);
    }
  }
}
