package io.rtr.conduit.amqp;

/** An action which the transport should take after returning from the handle callback. */
public class ActionResponse {

    private Action action;
    private String reason;
    public static final String REASON_KEY = "reason";

    private ActionResponse(Action action, String reason) {
        this.action = action;
        this.reason = reason;
    }

    public static ActionResponse acknowledge() {
        return new ActionResponse(Action.Acknowledge, null);
    }

    public static ActionResponse retry() {
        return new ActionResponse(Action.RejectAndRequeue, null);
    }

    public static ActionResponse retry(String reason, Object... args) {
        return new ActionResponse(Action.RejectAndRequeue, String.format(reason, args));
    }

    public static ActionResponse discard() {
        return new ActionResponse(Action.RejectAndDiscard, null);
    }

    public static ActionResponse discard(String reason, Object... args) {
        return new ActionResponse(Action.RejectAndDiscard, String.format(reason, args));
    }

    public String getReason() {
        return reason;
    }

    public Action getAction() {
        return action;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ActionResponse that = (ActionResponse) o;

        if (action != that.action) return false;
        if (reason != null ? !reason.equals(that.reason) : that.reason != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = action != null ? action.hashCode() : 0;
        result = 31 * result + (reason != null ? reason.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ActionResponse{" + "action=" + action + ", reason='" + reason + '\'' + '}';
    }

    public enum Action {
        Acknowledge, // ! The transport will ack the message explicitly.
        RejectAndRequeue, // ! The message wasn't meant to be processed.
        //  For example, if the message delivered is of
        //  a higher version than what we are able to
        //  deal with.
        RejectAndDiscard // ! A malformed message, place it on a poison queue.
    }
}
