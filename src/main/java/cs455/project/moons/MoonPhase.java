package cs455.project.moons;

public enum MoonPhase {
    NEW_MOON(1, "New Moon"),
    FIRST_QUARTER(2, "First Quarter"),
    FULL_MOON(3, "Full Moon"),
    LAST_QUARTER(4, "Last Quarter");

    private final int phaseId;
    private final String name;

    MoonPhase(int phaseId, String name) {
        this.phaseId = phaseId;
        this.name = name;
    }

    public String getPrettyName() {
        return name;
    }

    public int getPhaseId() {
        return phaseId;
    }
}
