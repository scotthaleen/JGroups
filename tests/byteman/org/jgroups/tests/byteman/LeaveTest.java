package org.jgroups.tests.byteman;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.*;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Tests graceful leaves of multiple members, especially coord and next-in-line.
 * Nodes are leaving gracefully so no merging is expected.<br/
 * Reproducer for https://issues.jboss.org/browse/JGRP-2293.
 *
 * @author Radoslav Husar
 * @author Bela Ban
 */
@Test(groups=Global.BYTEMAN,singleThreaded=true)
public class LeaveTest /*extends BMNGRunner */ {

    protected static final int NUM = 10;
    protected static final InetAddress LOOPBACK;

    static {
        try {
            LOOPBACK = Util.getLocalhost();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected JChannel[] channels = new JChannel[NUM];

    @BeforeMethod
    protected void setup() throws Exception {
        for(int i = 0; i < channels.length; i++) {
            channels[i] = create(String.valueOf(i + 1)).connect(LeaveTest.class.getSimpleName());
            System.out.printf("%d ", i+1);
            Util.sleep(i < 1 ? 2000 : 100);
        }
        System.out.println("\n");
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, channels);
    }

    @AfterMethod protected void destroy() {
        Util.closeReverse(channels);
        assert Stream.of(channels).allMatch(JChannel::isClosed);
        System.out.println("\n\n================================================================\n\n");
    }

    /** A single member (coord) leaves */
    public void testLeaveOfSingletonCoord() throws Exception {
        JChannel x=null;
        destroy();
        try {
            x=create("X").connect("x-cluster");
            assert x.getView().size() == 1;
            Util.close(x);
            assert x.getView() == null;
        }
        finally {
            Util.close(x);
        }
    }

    /** The coord leaves */
    public void testCoordLeave() {
        Util.close(channels[0]);
        assert Arrays.stream(channels, 0, NUM).filter(JChannel::isConnected)
          .peek(ch -> System.out.printf("%s: %s\n", ch.getAddress(), ch.getView()))
          .allMatch(ch -> ch.getView().size() == NUM-1 && ch.getView().getCoord().equals(channels[1].getAddress()));
    }

    /** A participant leaves */
    public void testParticipantLeave() {
        Util.close(channels[2]);
        assert Arrays.stream(channels, 0, NUM).filter(JChannel::isConnected)
          .peek(ch -> System.out.printf("%s: %s\n", ch.getAddress(), ch.getView()))
          .allMatch(ch -> ch.getView().size() == NUM-1 && ch.getView().getCoord().equals(channels[0].getAddress()));
    }

    /** The first N coords leave, one after the other */
    public void testSequentialLeavesOfCoordinators() {
        Arrays.stream(channels, 0, NUM/2).forEach(Util::close);
        Arrays.stream(channels, 0, NUM).forEach(ch -> {
            if(ch.isConnected())
                System.out.printf("%s: %s\n", ch.getAddress(), ch.getView());
        });
        Address coord=channels[NUM/2].getAddress();
        System.out.printf("-- new coord is %s\n", coord);
        assert Arrays.stream(channels, NUM/2, NUM)
          .allMatch(ch -> ch.getView().size() == NUM/2 && ch.getView().getCoord().equals(coord));
    }

    /** The coord and next-coord leave concurrently (next leaves first) */
    // @Test(invocationCount=2)
    public void testLeaveOfNextAndCoord() throws Exception {
        testLeaveOfFirstNMembers(Comparator.comparingInt(GmsImpl.Request::getType), 2);
    }

    /** The coord and next N members concurrently (next leaves first) */
    public void testLeaveOfNext8AndCoord() throws Exception {
        testLeaveOfFirstNMembers(Comparator.comparingInt(GmsImpl.Request::getType), 8);
    }

    /** The coord and next-coord leave concurrently (coord leaves first) */
    @Test(invocationCount=2)
    public void testLeaveOfCoordAndNext() throws Exception {
        testLeaveOfFirstNMembers(Comparator.comparingInt(GmsImpl.Request::getType).reversed(), 2);
    }

    /** The first NUM_LEAVERS leave concurrently */
    public void testConcurrentLeaves2() throws Exception {
        testConcurrentLeaves(2);
    }

    /** The first NUM_LEAVERS leave concurrently */
    public void testConcurrentLeaves8() throws Exception {
        testConcurrentLeaves(8);
    }

    /** The first num_leavers leave concurrently */
    protected void testConcurrentLeaves(int num_leavers) throws Exception {
        JChannel[] remaining_channels=new JChannel[channels.length - num_leavers];
        System.arraycopy(channels, num_leavers, remaining_channels, 0, channels.length - num_leavers);
        Stream.of(channels).limit(num_leavers).parallel().forEach(Util::close);
        Util.waitUntilAllChannelsHaveSameView(30000, 1000, remaining_channels);
        Arrays.stream(channels, 0, NUM).filter(JChannel::isConnected)
          .forEach(ch -> System.out.printf("%s: %s\n", ch.getAddress(), ch.getView()));
    }


    /** Sorts and delivers requests LEAVE and COORD_LEAVE according to parameter 'comp' */
    protected void testLeaveOfFirstNMembers(Comparator<GmsImpl.Request> comp, int leavers) throws Exception {
        GMS gms=channels[0].getProtocolStack().findProtocol(GMS.class);
        ViewHandler vh=gms.getViewHandler();
        MyRequestProcessor rp=new MyRequestProcessor(vh.reqProcessor(), comp, leavers);
        vh.reqProcessor(rp);

        testConcurrentLeaves(leavers);
        rp.passThrough(true);
        assert Arrays.stream(channels, 0, leavers).allMatch(ch -> ch.getView() == null);
        assert Arrays.stream(channels, leavers, NUM - 1)
          .allMatch(ch -> ch.getView().size() == NUM - leavers && ch.getView().getCoord().equals(channels[leavers].getAddress()));
    }


    protected static JChannel create(String name) throws Exception {
        return new JChannel(
          new TCP().setBindAddress(LOOPBACK).setBindPort(7800),
          new MPING(),
          // new TCPPING().portRange(9).initialHosts(Collections.singletonList(new InetSocketAddress("127.0.0.1", 7800))),

          //new UDP().setBindAddress(LOOPBACK),
          //new PING(),

          // new SHARED_LOOPBACK(),
          // new SHARED_LOOPBACK_PING(),

          // omit MERGE3 from the stack -- nodes are leaving gracefully
          // new MERGE3().setMinInterval(1000).setMaxInterval(3000).setCheckInterval(5000),
          new FD_SOCK(),
          new FD_ALL(),
          new VERIFY_SUSPECT(),
          new NAKACK2().setUseMcastXmit(false),
          new UNICAST3(),
          new STABLE(),
          new GMS().joinTimeout(1000).leaveTimeout(6000).printLocalAddress(false))
          .name(name);
    }

    protected static class MyRequestProcessor implements Consumer<Collection<GmsImpl.Request>> {
        protected final Consumer<Collection<GmsImpl.Request>> old_processor;
        protected final List<GmsImpl.Request>                 reqs=new ArrayList<>();
        protected final Comparator<GmsImpl.Request>           comparator;
        protected final int                                   max_reqs;
        protected boolean                                     pass_through;

        public MyRequestProcessor(Consumer<Collection<GmsImpl.Request>> old_processor,
                                  Comparator<GmsImpl.Request> comparator, int max_reqs) {
            this.old_processor=old_processor;
            this.comparator=comparator;
            this.max_reqs=max_reqs;
        }

        protected MyRequestProcessor passThrough(boolean pt) {this.pass_through=pt; return this;}

        // queue LEAVE and COORD_LEAVE until both have been received, deliver in predefined order
        public void accept(Collection<GmsImpl.Request> requests) {
            if(pass_through) {
                old_processor.accept(requests);
                return;
            }

            requests.stream().filter(r -> r.getType() == GmsImpl.Request.LEAVE || r.getType() == GmsImpl.Request.COORD_LEAVE)
              .forEach(reqs::add);

            if(reqs.size() >= max_reqs) {
                reqs.sort(comparator);
                System.out.printf("-- sorted requests to: %s\n", reqs);
                for(GmsImpl.Request r: reqs)
                    old_processor.accept(Collections.singletonList(r));
                reqs.clear();
                passThrough(true); // todo: ????
            }
        }
    }
}
