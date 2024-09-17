[@@@ocaml.warning "-32-27-26"]

open Fun_db

let () = print_endline "FunDB started"

let exec c cmd args =
  match Fun_db.Connection.exec_command c cmd args with
    | Ok((c, r)) -> (c, r)
    | Error(e) -> (c, e)

let test_read_uncommitted () =
  print_endline "Read Uncommitted";
  let db = Fun_db.Database.make () in
  let db = {db with default_isolation = ReadUncommitted} in
  let c1 = Fun_db.Connection.make db in

  let (c1, r) = exec c1 "begin" [] in
  print_endline r;
  assert (r = "1");
  print_endline @@ Int.to_string c1.id;
  print_endline @@ Int.to_string c1.db.next_transaction_id;
  assert (c1.db.next_transaction_id = 2);

  let c2 = Fun_db.Connection.make c1.db in
  let (c2, r) = exec c2 "begin" [] in

  let (c1, r) = exec c1 "set" ["a"; "1"] in
  print_endline r;
  assert (r = "1");

  (* update is visible to itself *)
  let (c1, r) = exec c1 "get" ["a"] in
  print_endline r;
  assert (r = "1");

  (* update is visible to other readuncommitted tx *)
  let (c2, r) = exec c2 "get" ["a"] in
  print_endline r;
  assert (r = "1");

  let (c1, r) = exec c1 "set" ["a"; "2"] in
  print_endline r;
  assert (r = "2");

  let (c1, r) = exec c1 "get" ["a"] in
  print_endline r;
  assert (r = "2");

  let (c1, r) = exec c1 "delete" ["a"] in
  print_endline r;
  assert (r = "");

  let (c1, r) = exec c1 "get" ["a"] in
  print_endline r;
  assert (r = "");

  let (c2, r) = exec c2 "get" ["a"] in
  print_endline r;
  assert (r = "");
  ()

let test_read_committed () =
  print_endline "Read Committed";
  let db = Database.make() in
  let db = {db with default_isolation = ReadCommitted} in

  let c1 = Connection.make db in
  let (c1, r) = exec c1 "begin" [] in

  let c2 = Connection.make c1.db in
  let (c2, r) = exec c2 "begin" [] in

  let (c1, r) = exec c1 "set" ["a"; "1"] in
  print_endline r;
  assert (r = "1");

  (* update is visible to itself *)
  let (c1, r) = exec c1 "get" ["a"] in
  print_endline r;
  assert (r = "1");

  (* update is not available to others *)
  let (c2, r) = exec c2 "get" ["a"] in
  print_endline r;
  assert (r <> "1");

  let (c1, r) = exec c1 "commit" [] in

  (* update is visible to others after commit *)
  let (c2, r) = exec c2 "get" ["a"] in
  print_endline r;
  assert (r = "1");

  let c3 = Connection.make c2.db in
  let (c3, r) = exec c3 "begin" [] in
  let (c3, r) = exec c3 "set" ["b"; "2"] in
  let (c3, r) = exec c3 "get" ["b"] in
  print_endline r;
  assert (r = "2");
  let (c3, r) = exec c3 "abort" [] in

  let (c2, r) = exec c2 "get" ["b"] in
  print_endline r;
  assert (r = "");

  let (c2, r) = exec c2 "delete" ["a"] in
  let (c2, r) = exec c2 "get" ["a"] in
  print_endline r;
  assert (r = "");

  let _ = exec c2 "commit" [] in

  let c4 = Connection.make c3.db in
  let (c4, _) = exec c4 "begin" [] in
  let (c4, r) = exec c4 "get" ["a"] in
  print_endline r;
  assert (r = "");

  ()

let test_repeatable_read () =
  print_endline "Repeatable Read";
  let db = Database.make() in
  let db = {db with default_isolation = RepeatableRead} in

  let c1 = Connection.make db in
  let (c1, r) = exec c1 "begin" [] in

  let c2 = Connection.make c1.db in
  let (c2, r) = exec c2 "begin" [] in

  let (c1, r) = exec c1 "set" ["a"; "1"] in
  print_endline r;
  assert (r = "1");

  (* update is visible to itself *)
  let (c1, r) = exec c1 "get" ["a"] in
  print_endline r;
  assert (r = "1");

  (* update is not available to others *)
  let (c2, r) = exec c2 "get" ["a"] in
  print_endline r;
  assert (r <> "1");

  let (c1, r) = exec c1 "commit" [] in

  (* update is not available to existing tx even after commit *)
  let (c2, r) = exec c2 "get" ["a"] in
  let _ = print_endline ("c2 get after commit of c1 " ^ r) in
  assert (r <> "1");

  (* update is visible to new transactions after commit*)
  let c3 = Connection.make c2.db in

  let (c3, r) = exec c3 "begin" [] in
  let (c3, r) = exec c3 "get" ["a"] in
  print_endline r;
  assert (r = "1");

  (* update is visible to itself *)
  let (c3, r) = exec c3 "set" ["b"; "2"] in
  let (c3, r) = exec c3 "get" ["b"] in
  print_endline r;
  assert (r = "2");
  let (c3, r) = exec c3 "abort" [] in

  (* update isn't visible to transactions after abort *)
  let (c2, r) = exec c2 "get" ["b"] in
  print_endline r;
  assert (r = "");

  (* delete is visible to itself *)
  let c4 = Connection.make c3.db in
  let (c4, _) = exec c4 "begin" [] in
  let (c4, r) = exec c4 "get" ["a"] in
  print_endline r;
  assert (r = "1");
  let (c4, r) = exec c4 "delete" ["a"] in
  print_endline r;
  assert (r = "");
  let (c4, r) = exec c4 "get" ["a"] in
  print_endline r;
  assert (r = "");

  let _ = exec c4 "commit" [] in

  (* delete is visible to new transaction after commit *)
  let c5 = Connection.make c4.db in
  let (c5, _) = exec c5 "begin" [] in
  let (c5, r) = exec c5 "get" ["a"] in
  print_endline r;
  assert (r = "");

  ()

let test_snapshot_isolation_write_write_conflict () =
  print_endline "Snapshot";

  let db = Database.make() in

  let db = {db with default_isolation = Snapshot} in

  let c1 = Connection.make db in
  let (c1, r) = exec c1 "begin" [] in

  let c2 = Connection.make c1.db in
  let (c2, r) = exec c2 "begin" [] in

  let (c1, r) = exec c1 "set" ["a"; "1"] in
  print_endline r;
  assert (r = "1");
  let _ = exec c1 "commit" [] in

  let (c2, r) = exec c2 "set" ["a"; "2"] in
  print_endline r;
  let _ = exec c2 "abort" [] in

  let c3 = Connection.make c2.db in
  let (c3, r) = exec c3 "begin" [] in
  let (c3, r) = exec c3 "get" ["a"] in
  print_endline r;
  assert (r = "1");

  let (c3, r) = exec c3 "set" ["b"; "2"] in
  print_endline r;
  assert (r = "2");
  let _ = exec c3 "commit" [] in

  let tx3 = Hashtbl.find db.transactions 3 in
  assert (tx3.state = Committed);

  let tx2 = Hashtbl.find db.transactions 2 in
  assert (tx2.state = Aborted);

  let tx1 = Hashtbl.find db.transactions 1 in
  assert (tx1.state = Committed);

  let v = match Hashtbl.find db.store "b" with | [v] -> Some v | _ -> None in
  let v = Option.get v in
  assert (v.value = "2");

  ()

let test_serializable_isolation_readwrite_conflict () =
  print_endline "Serializable";

  let db = Database.make() in

  let db = {db with default_isolation = Serializable} in

  let c1 = Connection.make db in
  let (c1, r) = exec c1 "begin" [] in

  let c2 = Connection.make c1.db in
  let (c2, r) = exec c2 "begin" [] in

  let c3 = Connection.make c2.db in
  let (c3, r) = exec c3 "begin" [] in

  let (c1, r) = exec c1 "set" ["a"; "1"] in
  print_endline r;
  assert (r = "1");
  let _ = exec c1 "commit" [] in

  let (c2, r) = exec c2 "get" ["a"] in
  print_endline r;
  assert (r = "");
  let _ = exec c2 "abort" [] in

  let (c3, r) = exec c3 "set" ["b"; "2"] in
  print_endline r;
  assert (r = "2");
  let _ = exec c3 "commit" [] in

  let tx3 = Hashtbl.find db.transactions 3 in
  assert (tx3.state = Committed);

  let tx2 = Hashtbl.find db.transactions 2 in
  assert (tx2.state = Aborted);

  let tx1 = Hashtbl.find db.transactions 1 in
  assert (tx1.state = Committed);

  let v = match Hashtbl.find db.store "b" with | [v] -> Some v | _ -> None in
  let v = Option.get v in
  assert (v.value = "2");

  ()

let () = test_read_uncommitted ()
let () = test_read_committed ()
let () = test_repeatable_read ()
let () = test_snapshot_isolation_write_write_conflict ()
let () = test_serializable_isolation_readwrite_conflict ()
