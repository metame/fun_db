[@@@ocaml.warning "-27-34-37-69"]

let ( let* ) = Result.bind

let assert_eq a b prefix =
  if a = b
  then Ok(true)
  else Error("a b neq " ^ prefix)

 (* var DEBUG = slices.Contains(os.Args, "--debug") *)

let _debug a =
  (* if DEBUG = false { return } *)

  let args = ["hey"; "you"; "guys"; a] in
  print_endline @@ List.fold_right (^) args ""

type value = {
    tx_start_id : int;
    tx_end_id : int;
    value : string;
  }

type transaction_state = InProgress | Aborted | Committed

type isolation_level =
  ReadUncommitted
| ReadCommitted
| RepeatableRead
| Snapshot
| Serializable

module IntSet = Set.Make(Int)
module StringSet = Set.Make(String)

type transaction = {
    isolation : isolation_level;
    id : int;
    state: transaction_state;
    (* Used only by Repeatable Read and stricter. *)
    inprogress: IntSet.t;
   (*  Used only by Snapshot Isolation and stricter. *)
    writeset: StringSet.t;
    readset: StringSet.t;
  }

module Database =
  struct
    type t = {
        default_isolation : isolation_level;
        store : (string, value list) Hashtbl.t;
        transactions : (int, transaction) Hashtbl.t;
        next_transaction_id : int;
      }

    let make () = {
        default_isolation = ReadCommitted;
        store = Hashtbl.create 100;
        transactions = Hashtbl.create 100;
        next_transaction_id = 1;
      }

    let in_progress db =
      Hashtbl.fold
        (fun id tx ids -> if tx.state = InProgress then IntSet.add id ids else ids)
        db.transactions
        IntSet.empty

    let new_transaction db =
      let tx = {
          isolation = db.default_isolation;
          state = InProgress;
          id = db.next_transaction_id;
          inprogress = in_progress db;
          writeset = StringSet.empty;
          readset = StringSet.empty;
        } in
      let db = {db with next_transaction_id = db.next_transaction_id + 1} in
      Hashtbl.add db.transactions tx.id tx;
      (db, tx)

    let txs_after db tx_id =
      let tx_ids = Hashtbl.to_seq_keys db.transactions in
      let tx_ids = List.to_seq @@ List.sort Int.compare @@ List.of_seq tx_ids in
      let tx_ids = List.of_seq @@ Seq.drop_while (( >= ) tx_id) tx_ids in
      tx_ids

    let has_conflict (db: t) (tx: transaction) (p: transaction -> transaction -> bool) =
      let test = fun tx2_id ->
        let tx2 = Hashtbl.find db.transactions tx2_id in
        tx2.state = Committed && p tx tx2 in

      let ip_conflict = List.find_opt test (IntSet.to_list tx.inprogress) in
      let post_conflict = List.find_opt test (txs_after db tx.id) in
      Option.is_some ip_conflict || Option.is_some post_conflict

    let first_item s1 =
      match StringSet.to_list s1 with [] -> "" | v :: _ -> v

    let sets_share_item s1 s2 = not (StringSet.disjoint s1 s2)

    (* TODO: this should return a result *)
    let rec complete_transaction db tx state =
      (* debug("completing transaction ", t.id) *)
      if state = Committed
         && ((tx.isolation = Snapshot
              && has_conflict db tx (fun tx1 tx2 -> sets_share_item tx1.writeset tx2.writeset))
             || (tx.isolation = Serializable
                 && has_conflict db tx (fun tx1 tx2 -> sets_share_item tx1.readset tx2.writeset
                                                       || sets_share_item tx1.writeset tx2.readset)))

                         (* Error "write-write conflict" *)
      then let _ = print_endline @@ "complete tx failed for id " ^ Int.to_string tx.id in
           complete_transaction db tx Aborted
      else let tx = {tx with state = state} in
      Hashtbl.replace db.transactions tx.id tx

    (* should just change name to get_transaction *)
    let transaction_state db tx_id =
      let opt_tx = Hashtbl.find_opt db.transactions tx_id in
      Option.to_result ~none:"invalid transaction" opt_tx

    let update_transaction db tx =
      Hashtbl.replace db.transactions tx.id tx;
      tx

    let assert_valid_transaction db tx_opt =
      let* tx = Option.to_result ~none:"transaction is None" tx_opt in
      let* _ = if tx.id > 0 then Ok(true) else Error("invalid id") in
      let* tx = transaction_state db tx.id in
      match tx.state with
      | InProgress -> Ok tx
      | _ -> Error "transaction not in progress"

    let is_read_committed_visible db tx v =
      let start_tx_c = transaction_state db v.tx_start_id
                       |> Result.map (fun tx -> tx.state = Committed)
                       |> Result.get_ok in
      let end_tx_c_result = transaction_state db v.tx_end_id
                            |> Result.map (fun tx -> tx.state = Committed) in
      not (v.tx_start_id <> tx.id && not start_tx_c)
      && not (v.tx_end_id = tx.id)
      && not (v.tx_end_id > 0 && Result.get_ok end_tx_c_result)

    (* TODO refactor this to use not && *)
    let is_repeatable_read_visible db tx v =
      if v.tx_start_id > tx.id
      then false
      else if Option.is_some (IntSet.find_opt v.tx_start_id tx.inprogress)
      then false
      else if v.tx_start_id <> tx.id
              && (Result.get_ok (transaction_state db v.tx_start_id)).state <> Committed
      then false
      else if v.tx_end_id = tx.id
      then false
      else if v.tx_end_id < tx.id
              && v.tx_end_id > 0
              && (Result.get_ok (transaction_state db v.tx_end_id)).state = Committed
              && Option.is_none (IntSet.find_opt v.tx_end_id tx.inprogress)
      then false
      else true

    let is_visible db tx v =
      match tx.isolation with
      | ReadUncommitted -> v.tx_end_id = 0
      | ReadCommitted -> is_read_committed_visible db tx v
      | _ -> is_repeatable_read_visible db tx v


  end

module Connection =
  struct
    type t = {
        id : int;
        tx : transaction option;
        db : Database.t;
      }

    let make (db: Database.t) = {
        db;
        id = db.next_transaction_id;
        tx = None;
      }

    let begin_tx c =
      let* _ = assert_eq c.tx None "no running transactions" in
      let (db, tx) = Database.new_transaction c.db in
      let* tx = Database.assert_valid_transaction db (Some tx) in
      Ok(({c with db = db; tx = Some tx}, Int.to_string tx.id))

    let abort_tx c =
      let* tx = Database.assert_valid_transaction c.db c.tx in
      Database.complete_transaction c.db tx Aborted;
      Ok({c with tx = None}, "")

    let commit_tx c =
      let* tx = Database.assert_valid_transaction c.db c.tx in
      Database.complete_transaction c.db tx Committed;
      Ok({c with tx = None}, "")

    let get c args =
      print_endline @@ "get on connection " ^ Int.to_string c.id;
      let* tx = Database.assert_valid_transaction c.db c.tx in
      let* key = Option.to_result ~none:"no key provided" (List.nth_opt args 0) in
      let readset = StringSet.add key tx.readset in
      let tx = Database.update_transaction c.db {tx with readset = readset} in
      let vs = Hashtbl.find_opt c.db.store key in
      let* vs = Option.to_result ~none:"no value for key exists" vs in
      (* debug(value, c.tx, c.db.isvisible(c.tx, value)) *)
      let v = List.find_opt (Database.is_visible c.db tx) vs in
      let v = Option.map (fun v -> v.value) v in
      let v = Option.value ~default:"" v in
      Ok({c with tx = Some(tx)}, v)

    let invalidate_versions db tx vs =
      List.fold_left_map (fun f v -> if Database.is_visible db tx v
                                      then (true, {v with tx_end_id = tx.id})
                                      else (f, v))
        false
        vs

    let set c args =
      let* tx = Database.assert_valid_transaction c.db c.tx in
      let* key = Option.to_result ~none:"no key provided" (List.nth_opt args 0) in
      let vs = Option.value ~default:[] @@ Hashtbl.find_opt c.db.store key in
      let (_, vs) = invalidate_versions c.db tx vs in
      let writeset = StringSet.add key tx.writeset in
      let tx = Database.update_transaction c.db {tx with writeset = writeset} in
      let* value = Option.to_result ~none:"no val provided" (List.nth_opt args 1) in
      let value = {tx_start_id = tx.id; tx_end_id = 0; value} in
      Hashtbl.replace c.db.store key (value :: vs);
      Ok({c with tx = Some(tx)}, value.value)

    let delete c args =
      let* tx = Database.assert_valid_transaction c.db c.tx in
      let* key = Option.to_result ~none:"no key provided" (List.nth_opt args 0) in
      let vs = Hashtbl.find_opt c.db.store key in
      let* vs = Option.to_result ~none:"no value for key exists" vs in
      let (found, vs) = invalidate_versions c.db tx vs in
      let* _ = if found then Ok(()) else Error("cannot delete key that does not exist") in
      let writeset = StringSet.add key tx.writeset in
      let tx = Database.update_transaction c.db {tx with writeset = writeset} in
      Hashtbl.replace c.db.store key vs;
      Ok({c with tx = Some(tx)}, "")

    let exec_command c command args =
      (* debug(command, args) *)
      match command with
      | "begin" -> begin_tx c
      | "abort" -> abort_tx c
      | "commit" -> commit_tx c
      | "get" -> get c args
      | "set" -> set c args
      | "delete" -> delete c args
      | _ -> Error "unimplemented"

    let must_exec_command c command args =
      let r = exec_command c command args in
      match r with
      | Ok(r) -> Ok r
      | Error(e) -> Error ("unexpected error " ^ e)

  end
