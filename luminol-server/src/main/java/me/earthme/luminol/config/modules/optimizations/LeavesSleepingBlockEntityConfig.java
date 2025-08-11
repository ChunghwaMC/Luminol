package me.earthme.luminol.config.modules.optimizations;

import me.earthme.luminol.config.EnumConfigCategory;
import me.earthme.luminol.config.IConfigModule;
import me.earthme.luminol.config.flags.ConfigInfo;
import me.earthme.luminol.config.flags.HotReloadUnsupported;

public class LeavesSleepingBlockEntityConfig implements IConfigModule {
    @ConfigInfo(baseName = "enabled")
    @HotReloadUnsupported
    public static boolean enabled = true;

    @Override
    public EnumConfigCategory getCategory() {
        return EnumConfigCategory.OPTIMIZATIONS;
    }

    @Override
    public String getBaseName() {
        return "lithium_sleeping_block_entity";
    }
}
