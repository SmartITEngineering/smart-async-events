package com.smartitengineering.events.async.api.impl.akka.decorator;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.smartitengineering.events.async.api.EventPublisher;
import junit.framework.TestCase;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.Sequence;
import org.jmock.integration.junit3.JUnit3Mockery;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {

  private static final Mockery mockery = new JUnit3Mockery();
  private static final EventPublisher mock = mockery.mock(EventPublisher.class);

  public void testTypedActorBasedImplementationWithFailures() {
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
    EventPublisher publisher = new EventPublisherImpl();
    publisher.publishEvent("a", "a");
  }

  public static class Module extends AbstractModule {

    @Override
    protected void configure() {

      bind(EventPublisher.class).annotatedWith(Names.named("decorateePublisher")).toInstance(mock);
    }
  }
}
