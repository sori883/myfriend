import * as THREE from 'three'
import { VRM, VRMHumanBoneName } from '@pixiv/three-vrm'
import { EmotionType } from '@/features/messages/messages'
import {
  GESTURE_POSES,
  EMOTION_MOTION_CONFIG,
  type OscillationDef,
} from './motionConstants'

const DEG2RAD = Math.PI / 180

/** 度数からクォータニオンへ変換 */
const eulerDegToQuat = (
  pitch: number,
  yaw: number,
  roll: number
): THREE.Quaternion => {
  return new THREE.Quaternion().setFromEuler(
    new THREE.Euler(pitch * DEG2RAD, yaw * DEG2RAD, roll * DEG2RAD, 'YXZ')
  )
}

/**
 * 感情に応じたボディジェスチャーを制御するクラス
 *
 * idle_loop.vrma のアイドルアニメーション上に、
 * 回転オフセットを加算する。
 * AnimationMixer.update() の後、vrm.update() の前に applyGesture() を呼ぶ。
 *
 * mixer がアニメーションしないボーンでの累積を防ぐため、
 * 初回アクセス時にベース値を保存し、
 * 毎フレーム「ベース → ジェスチャー → オシレーション」の順で上書きする。
 */
export class MotionController {
  private _currentRotations: Map<VRMHumanBoneName, THREE.Quaternion>
  private _targetRotations: Map<VRMHumanBoneName, THREE.Quaternion>
  private _allUsedBones: ReadonlySet<VRMHumanBoneName>
  private _elapsedTime: number

  /** 初回アクセス時のボーンクォータニオン（累積防止用） */
  private _baseQuats: Map<VRMHumanBoneName, THREE.Quaternion>
  /** 現在の感情に対応するオシレーション定義 */
  private _currentOscillations: readonly OscillationDef[]
  /** 現在の感情に対応するSLERP補間速度 */
  private _slerpSpeed: number

  // _applyOscillation 用の再利用オブジェクト（同期呼び出しのため安全）
  private _tempQuat: THREE.Quaternion
  private _tempEuler: THREE.Euler

  constructor() {
    this._currentRotations = new Map()
    this._targetRotations = new Map()
    this._baseQuats = new Map()
    // 全ポーズで使われるボーンを事前計算
    this._allUsedBones = new Set(
      Object.values(GESTURE_POSES).flatMap((pose) =>
        pose.map((p) => p.bone)
      )
    )
    this._elapsedTime = 0
    this._tempQuat = new THREE.Quaternion()
    this._tempEuler = new THREE.Euler()

    // 初期状態は neutral
    const neutralConfig = EMOTION_MOTION_CONFIG.neutral
    this._currentOscillations = neutralConfig.oscillations
    this._slerpSpeed = neutralConfig.slerpSpeed
  }

  /** 目標の感情ポーズを設定（遷移は update() でスムーズに行われる） */
  public setEmotion(emotion: EmotionType): void {
    const pose = GESTURE_POSES[emotion]
    if (!pose) return

    const newTargets = new Map<VRMHumanBoneName, THREE.Quaternion>()
    const poseBones = new Set<VRMHumanBoneName>()

    for (const entry of pose) {
      newTargets.set(
        entry.bone,
        eulerDegToQuat(entry.pitch, entry.yaw, entry.roll)
      )
      poseBones.add(entry.bone)
    }

    // 新しいポーズにないボーンは identity に戻す
    for (const bone of this._allUsedBones) {
      if (!poseBones.has(bone)) {
        newTargets.set(bone, new THREE.Quaternion())
      }
    }

    this._targetRotations = newTargets

    // 感情別モーション設定を適用
    const config = EMOTION_MOTION_CONFIG[emotion]
    if (config) {
      this._currentOscillations = config.oscillations
      this._slerpSpeed = config.slerpSpeed
    }
  }

  /** 内部補間を進める（毎フレーム呼び出し） */
  public update(delta: number): void {
    this._elapsedTime += delta
    // 浮動小数点精度劣化を防止（全周波数の周期が収まる十分大きな値でmod）
    if (this._elapsedTime > 1000) {
      this._elapsedTime -= 1000
    }

    for (const [bone, target] of this._targetRotations) {
      let current = this._currentRotations.get(bone)
      if (!current) {
        current = new THREE.Quaternion()
        this._currentRotations.set(bone, current)
      }
      current.slerp(target, 1 - Math.exp(-this._slerpSpeed * delta))
    }
  }

  /**
   * ボーンにジェスチャーを適用する
   * mixer.update() の後、vrm.update() の前に呼ぶこと
   */
  public applyGesture(vrm: VRM): void {
    const humanoid = vrm.humanoid
    if (!humanoid) return

    // 1. ジェスチャー対象ボーンをベースクォータニオンにリセット
    for (const boneName of this._currentRotations.keys()) {
      this._resetBoneQuat(humanoid, boneName)
    }

    // 2. 回転オシレーション対象ボーンもベースにリセット
    for (const osc of this._currentOscillations) {
      this._resetBoneQuat(humanoid, osc.bone)
    }

    // 3. ジェスチャーオフセットを乗算
    for (const [boneName, offset] of this._currentRotations) {
      const boneNode = humanoid.getNormalizedBoneNode(boneName)
      if (!boneNode) continue
      boneNode.quaternion.multiply(offset)
    }

    // 4. 回転オシレーションを乗算
    for (const osc of this._currentOscillations) {
      this._applyOscillation(
        humanoid,
        osc.bone,
        osc.axis,
        osc.amplitudeDeg,
        osc.frequencyHz
      )
    }

  }

  /** ボーンのクォータニオンをベースにリセット（重複呼び出しは安全） */
  private _resetBoneQuat(
    humanoid: VRM['humanoid'],
    boneName: VRMHumanBoneName
  ): void {
    const boneNode = humanoid.getNormalizedBoneNode(boneName)
    if (!boneNode) return

    if (!this._baseQuats.has(boneName)) {
      this._baseQuats.set(boneName, boneNode.quaternion.clone())
    }
    boneNode.quaternion.copy(this._baseQuats.get(boneName)!)
  }

  private _applyOscillation(
    humanoid: VRM['humanoid'],
    bone: VRMHumanBoneName,
    axis: 'pitch' | 'yaw' | 'roll',
    amplitudeDeg: number,
    frequencyHz: number
  ): void {
    const boneNode = humanoid.getNormalizedBoneNode(bone)
    if (!boneNode) return

    const angle =
      Math.sin(this._elapsedTime * frequencyHz * Math.PI * 2) * amplitudeDeg

    this._tempEuler.set(
      axis === 'pitch' ? angle * DEG2RAD : 0,
      axis === 'yaw' ? angle * DEG2RAD : 0,
      axis === 'roll' ? angle * DEG2RAD : 0,
      'YXZ'
    )
    this._tempQuat.setFromEuler(this._tempEuler)
    boneNode.quaternion.multiply(this._tempQuat)
  }

}
