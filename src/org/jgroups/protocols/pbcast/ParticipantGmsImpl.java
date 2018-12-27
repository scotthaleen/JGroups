
package org.jgroups.protocols.pbcast;

import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.ViewId;
import org.jgroups.util.Digest;

import java.util.*;


/**
 * @author Bela Ban
 */
public class ParticipantGmsImpl extends ServerGmsImpl {
    private final Collection<Address> suspected_mbrs=new LinkedHashSet<>();


    public ParticipantGmsImpl(GMS g) {
        super(g);
    }


    public void init() throws Exception {
        super.init();
        suspected_mbrs.clear();
    }

    public void join(Address mbr, boolean useFlushIfPresent) {
        wrongMethod("join");
    }

    public void joinWithStateTransfer(Address mbr,boolean useFlushIfPresent) {
        wrongMethod("join");
    }


    public void leave(Address mbr) {
        if(sendLeaveReqToCoord(gms.determineCoordinator()))
            gms.initState();
    }


    /** In case we get a different JOIN_RSP from a previous JOIN_REQ sent by us (as a client), we simply apply the
     * new view if it is greater than ours
     */
    @Override
    public void handleJoinResponse(JoinRsp join_rsp) {
        View v=join_rsp.getView();
        ViewId tmp_vid=v != null? v.getViewId() : null;
        ViewId my_view=gms.getViewId();
        if(tmp_vid != null && my_view != null && tmp_vid.compareToIDs(my_view) > 0) {
            Digest d=join_rsp.getDigest();
            gms.installView(v, d);
        }
    }



    public void suspect(Address mbr) {
        Collection<Request> suspected=new LinkedHashSet<>(1);
        suspected.add(new Request(Request.SUSPECT, mbr));
        handleMembershipChange(suspected);
    }


    /** Removes previously suspected member from list of currently suspected members */
    public void unsuspect(Address mbr) {
        if(mbr != null)
            suspected_mbrs.remove(mbr);
    }


    public void handleMembershipChange(Collection<Request> requests) {
        Collection<Address> leaving_mbrs=new LinkedHashSet<>(requests.size());
        requests.forEach(r -> {
            if(r.type == Request.SUSPECT)
                suspected_mbrs.add(r.mbr);
            else if(r.type == Request.LEAVE)
                leaving_mbrs.add(r.mbr);
        });

        if(suspected_mbrs.isEmpty() && leaving_mbrs.isEmpty())
            return;

        if(wouldIBeCoordinator(leaving_mbrs)) {

            String add=gms.getImpl() instanceof ParticipantGmsImpl? " (becoming coordinator)": "";
            System.out.printf("**** %s (%s): handleMembershipChange(%s) %s\n",
                              gms.getLocalAddress(), gms.getImplementation(), requests, add);
            //System.out.printf("**** %s (%s): received leave reqs from %s\n",
              //                gms.getLocalAddress(), gms.getImplementation(), leaving_mbrs);

            log.debug("%s: members are %s, coord=%s: I'm the new coordinator", gms.local_addr, gms.members, gms.local_addr);
            boolean leaving=gms.isLeaving();
            gms.becomeCoordinator();
            Collection<Request> leavingOrSuspectedMembers=new LinkedHashSet<>();
            leaving_mbrs.forEach(mbr -> leavingOrSuspectedMembers.add(new Request(Request.LEAVE, mbr)));
            suspected_mbrs.forEach(mbr -> {
                leavingOrSuspectedMembers.add(new Request(Request.SUSPECT, mbr));
                gms.ack_collector.suspect(mbr);
            });
            suspected_mbrs.clear();
            if(leaving)
                leavingOrSuspectedMembers.add(new Request(Request.COORD_LEAVE, gms.local_addr));
            long start=System.currentTimeMillis();
            gms.getViewHandler().add(leavingOrSuspectedMembers);

            // If we're the coord leaving, ignore gms.leave_timeout: https://issues.jboss.org/browse/JGRP-1509
            //long timeout=(long)(Math.max(gms.leave_timeout, gms.view_ack_collection_timeout) * 1.10);
            //gms.getViewHandler().waitUntilComplete(timeout);
            //long time=System.currentTimeMillis()-start;
            //System.out.printf("**** %s (%s): waited for %d ms for leave(%s) to complete\n",
              //                gms.getLocalAddress(), gms.getImplementation(), time, leavingOrSuspectedMembers);


        }
    }


    /**
     * If we are leaving, we have to wait for the view change (last msg in the current view) that
     * excludes us before we can leave.
     * @param new_view The view to be installed
     * @param digest   If view is a MergeView, digest contains the seqno digest of all members and has to be set by GMS
     */
    public void handleViewChange(View new_view, Digest digest) {
        suspected_mbrs.clear();
        if(gms.isLeaving() && !new_view.containsMember(gms.local_addr)) // received a view in which I'm not member: ignore
            return;
        gms.installView(new_view, digest);
    }



    /* ---------------------------------- Private Methods --------------------------------------- */

    /**
     * Determines whether this member is the new coordinator given a list of suspected members.  This is
     * computed as follows: the list of currently suspected members (suspected_mbrs) is removed from the current
     * membership. If the first member of the resulting list is equals to the local_addr, then it is true,
     * otherwise false. Example: own address is B, current membership is {A, B, C, D}, suspected members are {A,
     * D}. The resulting list is {B, C}. The first member of {B, C} is B, which is equal to the
     * local_addr. Therefore, true is returned.
     */
    boolean wouldIBeCoordinator(Collection<Address> leaving_mbrs) {
        List<Address> mbrs=gms.computeNewMembership(gms.members.getMembers(), null, leaving_mbrs, suspected_mbrs);
        if(mbrs.isEmpty()) return false;
        Address new_coord=mbrs.get(0);
        return gms.local_addr.equals(new_coord);
    }


    /* ------------------------------ End of Private Methods ------------------------------------ */

}
