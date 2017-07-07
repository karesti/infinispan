package org.infinispan.commands.functional.functions;

import org.infinispan.factories.ComponentRegistry;

public interface InjectableWrappper {

   void inject(ComponentRegistry registry);
}
