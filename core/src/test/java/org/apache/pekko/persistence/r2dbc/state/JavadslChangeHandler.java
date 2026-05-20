/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package org.apache.pekko.persistence.r2dbc.state;

import pekko.Done;
import pekko.persistence.query.DurableStateChange;
import pekko.persistence.query.UpdatedDurableState;
import pekko.persistence.r2dbc.session.javadsl.R2dbcSession;
import pekko.persistence.r2dbc.state.javadsl.ChangeHandler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class JavadslChangeHandler implements ChangeHandler<String> {
  @Override
  public CompletionStage<Done> process(R2dbcSession session, DurableStateChange<String> change) {
    if (change instanceof UpdatedDurableState) {
      UpdatedDurableState<String> upd = (UpdatedDurableState<String>) change;
      return session
          .updateOne(
              session
                  .createStatement("insert into changes_test (pid, rev, value) values ($1, $2, $3)")
                  .bind(0, upd.persistenceId())
                  .bind(1, upd.revision())
                  .bind(2, upd.value()))
          .thenApply(n -> Done.getInstance());
    } else {
      return CompletableFuture.completedFuture(Done.getInstance());
    }
  }
}
